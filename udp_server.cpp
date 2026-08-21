// sendmmsg()/mmsghdr are GNU extensions in glibc headers; ensure they are
// declared even under a strict -std=c++17 build.
#define _GNU_SOURCE

#include <libobsensor/ObSensor.hpp>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <poll.h>
#include <condition_variable>

// ============================================================
// CONFIGURATION
// ============================================================
#define HOST_IP             "192.168.0.249"
#define STREAM_PORT         9999    // Video stream (out)
#define COMMAND_PORT        9998    // Mode switch commands (in)
#define MAX_CHUNK_SIZE      1400    // Fit single MTU (1500 - IP/UDP headers)
#define SEND_BATCH          32      // Datagrams per sendmmsg call
#define RING_SLOTS          2       // Capture <-> network handoff slots
// ============================================================

#define STREAM_COLOR  0
#define STREAM_DEPTH  1

#define MODE_CLOSE    0   // 1080p@30 + depth@30
#define MODE_FAR      1   // 4K@25 + depth@25

#pragma pack(push, 1)
struct PacketHeader {
    uint32_t frame_id;
    uint16_t chunk_index;
    uint16_t total_chunks;
    uint32_t data_size;
    uint8_t  stream_type;
    uint8_t  mode;          // Current active mode
    uint8_t  reserved[2];
};
#pragma pack(pop)

// === Shared State ===
std::atomic<bool> keep_running(true);
std::atomic<int>  requested_mode(MODE_CLOSE);
std::atomic<int>  active_mode(MODE_CLOSE);
std::atomic<bool> is_switching(false);
std::mutex cache_mutex;
std::vector<uint8_t> cached_color;
std::vector<uint8_t> cached_depth;
uint32_t cached_frame_id = 0;
int cached_mode = MODE_CLOSE;

// === Dynamic destination state ===
// Set when a receiver connects to the TCP command port (9998). The camera
// thread reads this atomic to determine the UDP stream destination.
// A value of 0 means "not registered" — falls back to HOST_IP.
std::atomic<in_addr_t> registered_receiver_ip(0);
std::atomic<bool>      receiver_connected(false);

inline sockaddr_in make_dest(in_addr_t ip) {
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(STREAM_PORT);
    if (ip != 0) {
        dest.sin_addr.s_addr = ip;
    } else {
        inet_pton(AF_INET, HOST_IP, &dest.sin_addr);
    }
    return dest;
}

// === Per-stream frame handoff ring (camera capture -> stream sender) ===
// One independent ring per stream (color, depth) so the large depth stream can
// never queue behind — and delay — the latency-critical color stream. The
// producer is NON-BLOCKING (latest-wins): when a ring is full it drops the
// OLDEST queued frame and writes the newest, so the camera thread never waits
// on the network — sender backpressure can no longer stall waitForFrameset.
// Dropped frames are expected under congestion (the receiver drops stale
// frames too), and the dropped counter surfaces that congestion.
struct FrameRing {
    static constexpr int SLOTS = RING_SLOTS;
    std::vector<std::vector<uint8_t>> data{SLOTS};
    std::vector<uint32_t> frame_id{SLOTS, 0};
    std::vector<uint8_t>  mode{SLOTS, 0};
    std::mutex mtx;
    std::condition_variable consume_cv;  // consumer waits: a frame is ready
    int head = 0;  // next slot the consumer will drain
    int tail = 0;  // next slot the producer will fill
    int count = 0;
    std::atomic<uint64_t> dropped{0};  // frames discarded on overflow

    // Non-blocking produce: evict the oldest frame when full, then store the
    // newest. Never waits — the camera thread stays on the wire cadence.
    void produce(const uint8_t* p, size_t n, uint32_t fid, uint8_t m) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (count == SLOTS) {          // full — evict the oldest
                head = (head + 1) % SLOTS;
                count--;
                dropped++;
            }
            data[tail].assign(p, p + n);
            frame_id[tail] = fid;
            mode[tail] = m;
            tail = (tail + 1) % SLOTS;
            count++;
        }
        consume_cv.notify_one();
    }

    // Blocking consume: O(1) vector swap out of the ring. Returns false only
    // on shutdown once the ring is drained.
    bool consume(std::vector<uint8_t>& out, uint32_t& fid, uint8_t& m) {
        std::unique_lock<std::mutex> lk(mtx);
        consume_cv.wait(lk, [&] { return count > 0 || !keep_running.load(); });
        if (count == 0) return false;      // shutdown with nothing left to send
        out.swap(data[head]);
        fid = frame_id[head];
        m = mode[head];
        head = (head + 1) % SLOTS;
        count--;
        return true;
    }
};

static FrameRing color_ring;
static FrameRing depth_ring;

struct ModeConfig {
    int color_w, color_h, color_fps;
    int depth_w, depth_h, depth_fps;
};

ModeConfig get_mode_config(int mode) {
    if (mode == MODE_FAR)
        return {3840, 2160, 25, 640, 576, 25};
    return {1920, 1080, 30, 640, 576, 30};
}

// === UDP Chunk Sender ===
// Sends one frame as total_chunks datagrams using sendmmsg batching over a
// zero-copy iovec (stack header + direct camera-buffer pointer). No per-chunk
// heap allocation, no payload memcpy. A 4K frame (~1500 chunks) costs ~47
// syscalls instead of ~1500.
void send_frame_chunks(int sockfd, const sockaddr_in& dest,
                       const uint8_t* data, uint32_t dataSize,
                       uint32_t frame_id, uint8_t stream_type, uint8_t mode) {
    if (dataSize == 0) return;
    uint16_t total_chunks = (dataSize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;

    mmsghdr msgs[SEND_BATCH];
    iovec   iov[SEND_BATCH * 2];
    PacketHeader hdr[SEND_BATCH];

    size_t offset = 0;
    uint16_t chunk_idx = 0;
    int in_batch = 0;

    while (offset < dataSize) {
        size_t chunk_len = std::min((size_t)MAX_CHUNK_SIZE, (size_t)(dataSize - offset));

        PacketHeader& h = hdr[in_batch];
        h.frame_id = htonl(frame_id);
        h.chunk_index = htons(chunk_idx);
        h.total_chunks = htons(total_chunks);
        h.data_size = htonl((uint32_t)chunk_len);
        h.stream_type = stream_type;
        h.mode = mode;
        memset(h.reserved, 0, sizeof(h.reserved));

        iov[in_batch * 2]     = { (void*)(&h), sizeof(h) };
        iov[in_batch * 2 + 1] = { (void*)(data + offset), chunk_len };

        msgs[in_batch].msg_hdr.msg_name = const_cast<sockaddr_in*>(&dest);
        msgs[in_batch].msg_hdr.msg_namelen = sizeof(dest);
        msgs[in_batch].msg_hdr.msg_iov = &iov[in_batch * 2];
        msgs[in_batch].msg_hdr.msg_iovlen = 2;
        msgs[in_batch].msg_hdr.msg_control = nullptr;
        msgs[in_batch].msg_hdr.msg_controllen = 0;
        msgs[in_batch].msg_hdr.msg_flags = 0;
        msgs[in_batch].msg_len = 0;

        in_batch++;
        chunk_idx++;
        offset += chunk_len;

        if (in_batch == SEND_BATCH || offset >= dataSize) {
            size_t sent = 0;
            while (sent < (size_t)in_batch) {
                ssize_t n = sendmmsg(sockfd, msgs + sent, in_batch - (int)sent, 0);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    return;  // fatal socket error — drop the rest of this frame
                }
                sent += (size_t)n;
            }
            in_batch = 0;
        }
    }
}

// === Stream sender thread ===
// One instance per stream (color, depth), each draining its own ring and
// pushing those frames to the wire. Blocking socket sends live here only; the
// ring absorbs the difference between camera cadence and wire speed, and the
// drop-oldest produce policy means a slow receiver drops stale frames instead
// of stalling the camera. Color and depth send concurrently on the shared
// sockfd (POSIX-safe) and are reassembled independently on the receiver.
void stream_sender_thread(int sockfd, std::atomic<in_addr_t>* dest_ip,
                          FrameRing& ring, uint8_t stream_type) {
    std::vector<uint8_t> outgoing;  // reused drain buffer — swap O(1) from ring
    const char* tag = (stream_type == STREAM_COLOR) ? "[NETC]" : "[NETD]";

    // FPS benchmark
    uint32_t frames_sent = 0;
    uint64_t total_bytes_sent = 0;
    auto fps_start = std::chrono::steady_clock::now();
    std::vector<float> drain_times;
    static const size_t DRAIN_WINDOW = 600;

    while (keep_running) {
        uint32_t fid = 0;
        uint8_t m = MODE_CLOSE;
        if (!ring.consume(outgoing, fid, m)) break;   // shutdown, ring drained

        sockaddr_in dest = make_dest(dest_ip->load());
        auto startSend = std::chrono::steady_clock::now();
        send_frame_chunks(sockfd, dest, outgoing.data(), (uint32_t)outgoing.size(),
                          fid, stream_type, m);
        total_bytes_sent += outgoing.size();
        auto endSend = std::chrono::steady_clock::now();
        float drain_ms = std::chrono::duration<float, std::milli>(
            endSend - startSend).count();
        drain_times.push_back(drain_ms);
        if (drain_times.size() > DRAIN_WINDOW)
            drain_times.erase(drain_times.begin());

        frames_sent++;
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - fps_start).count();
        if (elapsed >= 1.0) {
            double mbps = total_bytes_sent * 8.0 / 1024 / 1024;
            std::cout << tag << " "
                      << (m == MODE_FAR ? "FAR(4K@25)" : "CLOSE(1080p@30)")
                      << " | FPS: " << frames_sent
                      << " | Bandwidth: " << mbps << " Mbps"
                      << " | Dropped: " << ring.dropped.load()
                      << " | Frames: " << fid;
            if (drain_times.size() >= 30) {
                std::vector<float> sorted = drain_times;
                std::sort(sorted.begin(), sorted.end());
                float sum = 0.0f;
                for (float v : sorted) sum += v;
                std::cout << " | drain_avg=" << (sum / sorted.size())
                          << " drain_p95=" << sorted[size_t(sorted.size() * 0.95)];
            }
            std::cout << std::endl;
            frames_sent = 0;
            total_bytes_sent = 0;
            fps_start = now;
        }
    }
}

// === Freeze frame sender thread ===
void freeze_thread(int sockfd, std::atomic<in_addr_t>* dest_ip) {
    while (keep_running) {
        if (is_switching.load()) {
            sockaddr_in dest = make_dest(dest_ip->load());
            std::vector<uint8_t> color_copy, depth_copy;
            uint32_t fid;
            int mode;
            {
                std::lock_guard<std::mutex> lock(cache_mutex);
                color_copy = cached_color;
                depth_copy = cached_depth;
                fid = ++cached_frame_id;
                mode = cached_mode;
            }

            if (!color_copy.empty()) {
                send_frame_chunks(sockfd, dest, color_copy.data(),
                                  color_copy.size(), fid, STREAM_COLOR, mode);
            }
            if (!depth_copy.empty()) {
                send_frame_chunks(sockfd, dest, depth_copy.data(),
                                  depth_copy.size(), fid, STREAM_DEPTH, mode);
            }
            // std::cout << "[FRZ] Sent 1 frame pair" << std::endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(66)); // ~15fps
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

// === Command Listener Thread ===
// A TCP listener (was UDP) so a connect-per-command sender can push a
// newline-terminated "CLOSE"/"FAR" command with delivery guarantees. The loop
// poll()s the listening socket plus any connected clients so multiple hosts
// can still send (as fire-and-forget UDP allowed before), while a 200ms poll
// timeout keeps shutdown responsive to keep_running.
void command_thread() {
    int cmd_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (cmd_sock < 0) {
        std::cerr << "[CMD] Socket creation failed!" << std::endl;
        return;
    }

    // set options: REUSEADDR for fast rebinding
    int opt = 1;
    setsockopt(cmd_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // bind socket to port
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(COMMAND_PORT);

    if (bind(cmd_sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[CMD] Bind failed on port " << COMMAND_PORT << std::endl;
        close(cmd_sock);
        return;
    }

    if (listen(cmd_sock, 4) < 0) {
        std::cerr << "[CMD] Listen failed on port " << COMMAND_PORT << std::endl;
        close(cmd_sock);
        return;
    }

    std::cout << "[CMD] Listening (TCP) on port " << COMMAND_PORT << std::endl;

    static const int  MAX_CLIENTS = 4;      // cap concurrent senders
    static const size_t MAX_LINE  = 256;    // guard: drop oversized lines
    struct CmdClient {
        int fd;
        std::string buf;                    // partial newline-delimited line
    };
    std::vector<CmdClient> clients;

    auto handle_command = [](const std::string& line) {
        if (line == "CLOSE" && active_mode.load() != MODE_CLOSE) {
            std::cout << "[CMD] Received: CLOSE → switching to 1080p@30" << std::endl;
            requested_mode.store(MODE_CLOSE);
        } else if (line == "FAR" && active_mode.load() != MODE_FAR) {
            std::cout << "[CMD] Received: FAR → switching to 4K@25" << std::endl;
            requested_mode.store(MODE_FAR);
        }
    };

    while (keep_running) {
        // Rebuild the poll set each pass: listening socket + all clients.
        std::vector<pollfd> fds;
        fds.reserve(clients.size() + 1);
        fds.push_back({cmd_sock, POLLIN, 0});
        for (const auto& c : clients)
            fds.push_back({c.fd, POLLIN, 0});

        int nready = poll(fds.data(), (nfds_t)fds.size(), 200); // 200ms timeout
        if (nready < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (nready == 0) continue;  // timeout — re-check keep_running

        // New connection(s): accept one per poll pass; leftovers stay readable.
        if (fds[0].revents & POLLIN) {
            sockaddr_in peer{};
            socklen_t plen = sizeof(peer);
            int cfd = accept(cmd_sock, (sockaddr*)&peer, &plen);
            if (cfd >= 0) {
                if (clients.size() >= (size_t)MAX_CLIENTS)
                    close(cfd);                     // at cap — drop the newest
                else {
                    registered_receiver_ip.store(peer.sin_addr.s_addr);
                    receiver_connected.store(true);
                    std::cout << "[CMD] Receiver registered from "
                              << inet_ntoa(peer.sin_addr)
                              << " (port " << ntohs(peer.sin_port) << ")"
                              << std::endl;
                    send(cfd, "OK\n", 3, 0);        // registration confirmation
                    clients.push_back({cfd, {}});
                }
            }
        }

        // Drain each client (iterate backwards so erases are cheap).
        for (int i = (int)clients.size() - 1; i >= 0; i--) {
            short revents = fds[i + 1].revents;
            if (revents & (POLLERR | POLLNVAL)) {
                close(clients[i].fd);
                clients.erase(clients.begin() + i);
                continue;
            }
            if (!(revents & (POLLIN | POLLHUP))) continue;

            char buf[256];
            ssize_t n = recv(clients[i].fd, buf, sizeof(buf), 0);
            if (n > 0)
                clients[i].buf.append(buf, (size_t)n);

            // Split buffered bytes into newline-terminated commands.
            size_t start = 0;
            size_t end;
            while ((end = clients[i].buf.find('\n', start)) != std::string::npos) {
                std::string line = clients[i].buf.substr(start, end - start);
                start = end + 1;
                while (!line.empty() && (line.back() == '\r' || line.back() == ' '))
                    line.pop_back();   // trim CR/spaces (newline already consumed)
                handle_command(line);
            }
            if (start > 0)
                clients[i].buf.erase(0, start);

            // Oversize guard: line without a newline for too long → drop.
            if (clients[i].buf.size() >= MAX_LINE) {
                close(clients[i].fd);
                clients.erase(clients.begin() + i);
                continue;
            }

            // Peer closed the connection. With connect-per-command an EOF may
            // carry a final command without a trailing newline — flush it.
            if (n == 0 || (revents & POLLHUP)) {
                if (n == 0 && !clients[i].buf.empty()) {
                    std::string line = clients[i].buf;
                    while (!line.empty() && (line.back() == '\r' ||
                                             line.back() == ' '))
                        line.pop_back();
                    handle_command(line);
                }
                close(clients[i].fd);
                clients.erase(clients.begin() + i);
                if (clients.empty()) {
                    receiver_connected.store(false);
                    std::cout << "[CMD] Receiver disconnected" << std::endl;
                }
            }
        }
    }

    for (auto& c : clients)
        close(c.fd);
    close(cmd_sock);
}

// === Camera Thread ===
void camera_thread() {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) { std::cerr << "[NET] Socket failed!" << std::endl; return; }

    // make big 4MB buffer
    int sndbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    uint32_t frame_id = 0;
    std::thread frz_thread(freeze_thread, sockfd, &registered_receiver_ip);
    std::thread col_net(stream_sender_thread, sockfd, &registered_receiver_ip, std::ref(color_ring), STREAM_COLOR);
    std::thread dep_net(stream_sender_thread, sockfd, &registered_receiver_ip, std::ref(depth_ring), STREAM_DEPTH);

    // Context persists for the thread's life; devices are queried per re-acquire.
    ob::Context context;

    // Capture-side benchmark: counts frames handed to the sender threads only.
    // Wire FPS / bandwidth / drain time are reported by the stream senders.
    uint32_t frames_captured = 0;
    auto cam_fps_start = std::chrono::steady_clock::now();

    // Frame-interval diagnostic: measures the delta between consecutive
    // waitForFrameset() returns (the camera's own frame cadence). If this
    // p95 is ~40ms while the raw receive-side gap is ~40ms, the residual
    // jitter is the Orbbec camera clock, not the sender upload or socket.
    static const size_t DIAG_WINDOW = 600; // ~20 s at 30 fps
    std::vector<float> frame_intervals;    // ms, capped at DIAG_WINDOW
    std::chrono::steady_clock::time_point prev_frame_start;

    while (keep_running) {
        // Apply mode switch if requested
        int current_mode = requested_mode.load();
        active_mode.store(current_mode);
        ModeConfig cfg = get_mode_config(current_mode);

        try {
            auto devList = context.queryDeviceList();
            if (!devList || devList->deviceCount() == 0) {
                std::cerr << "[CAM] No devices found, retrying..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            auto device = devList->getDevice(0);

            std::shared_ptr<ob::StreamProfile> colorProfile = nullptr;
            std::shared_ptr<ob::StreamProfile> depthProfile = nullptr;
            auto sensorList = device->getSensorList();

            for (uint32_t i = 0; i < sensorList->count(); i++) {
                auto sensor = sensorList->getSensor(i);
                auto profiles = sensor->getStreamProfileList();

                if (sensor->getType() == OB_SENSOR_COLOR) {
                    for (uint32_t j = 0; j < profiles->count(); j++) {
                        auto p = profiles->getProfile(j);
                        auto vp = p->as<ob::VideoStreamProfile>();
                        if (vp && vp->width() == cfg.color_w && vp->height() == cfg.color_h
                            && vp->fps() == cfg.color_fps && vp->format() == OB_FORMAT_MJPEG) {
                            colorProfile = p;
                            break;
                        }
                    }
                }
                if (sensor->getType() == OB_SENSOR_DEPTH) {
                    for (uint32_t j = 0; j < profiles->count(); j++) {
                        auto p = profiles->getProfile(j);
                        auto vp = p->as<ob::VideoStreamProfile>();
                        if (vp && vp->width() == cfg.depth_w && vp->height() == cfg.depth_h
                            && vp->fps() == cfg.depth_fps && vp->format() == OB_FORMAT_Y16) {
                            depthProfile = p;
                            break;
                        }
                    }
                }
            }

            if (!colorProfile || !depthProfile) {
                std::cerr << "[CAM] Profile not found for mode " << current_mode << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            auto colorConfig = std::make_shared<ob::Config>();
            colorConfig->enableStream(colorProfile);
            auto depthConfig = std::make_shared<ob::Config>();
            depthConfig->enableStream(depthProfile);

            // One pipeline per sensor: each FrameSet carries exactly that
            // sensor's frame, produced on its own cadence. No aggregate mode
            // needed — a single-stream pipeline cannot hold frames back.
            ob::Pipeline color_pipe(device);
            ob::Pipeline depth_pipe(device);
            color_pipe.start(colorConfig);
            depth_pipe.start(depthConfig);
            is_switching.store(false);
            sockaddr_in dest = make_dest(registered_receiver_ip.load());
            std::cout << "[CAM] Streaming: " << cfg.color_w << "x" << cfg.color_h
                      << "@" << cfg.color_fps << " + "
                      << cfg.depth_w << "x" << cfg.depth_h
                      << "@" << cfg.depth_fps
                      << " → " << inet_ntoa(dest.sin_addr) << ":"
                      << STREAM_PORT
                      << (receiver_connected.load()
                              ? "" : " (no receiver registered, using fallback)")
                      << std::endl;

            // Watchdog: track last successful frame time per pipeline
            auto last_color_time = std::chrono::steady_clock::now();
            auto last_depth_time = std::chrono::steady_clock::now();

            // Freeze-frame cache lite refresh: while streaming we do NOT copy
            // every frame into the cache (that was a hot-path memcpy). Instead
            // we refresh it every CACHE_REFRESH_FRAMES so a mode switch's
            // freeze-bridge always re-sends a fresh-ish frame (<=1s stale).
            const int CACHE_REFRESH_FRAMES = 30;
            int cache_refresh_counter = 0;

            // === Frame loop ===
            while (keep_running) {
                if (requested_mode.load() != current_mode) break;  // mode switch

                auto frame_t0 = std::chrono::steady_clock::now();
                auto colorSet = color_pipe.waitForFrameset(100);
                auto depthSet = depth_pipe.waitForFrameset(100);

                auto colorFrame = colorSet ? colorSet->colorFrame() : nullptr;
                auto depthFrame = depthSet ? depthSet->depthFrame() : nullptr;

                auto stall_now = std::chrono::steady_clock::now();
                if (colorFrame) last_color_time = stall_now;
                if (depthFrame) last_depth_time = stall_now;

                // Watchdog: no frames from a stream for 3 seconds -> break to
                // restart the pipeline. Each stream is tracked independently so
                // a stalled sensor triggers recovery even if the other is alive.
                int color_stall_s = std::chrono::duration_cast<std::chrono::seconds>(stall_now - last_color_time).count();
                int depth_stall_s = std::chrono::duration_cast<std::chrono::seconds>(stall_now - last_depth_time).count();
                if (color_stall_s > 3 || depth_stall_s > 3) {
                    std::cerr << "[CAM] Watchdog: stream silent for " << std::max(color_stall_s, depth_stall_s)
                              << "s, breaking to restart pipeline" << std::endl;
                    break;
                }

                if (!colorFrame && !depthFrame) {
                    continue;   // nothing to send this pass
                }

                // Record the camera frame-cadence delta (only once we have a
                // previous frame to measure against).
                if (frame_id > 0) {
                    float dt_ms = std::chrono::duration<float, std::milli>(
                        frame_t0 - prev_frame_start).count();
                    frame_intervals.push_back(dt_ms);
                    if (frame_intervals.size() > DIAG_WINDOW)
                        frame_intervals.erase(frame_intervals.begin());
                }
                prev_frame_start = frame_t0;

                // Each stream is handed off under the same pair frame_id; the
                // receiver releases on colour and attaches the freshest complete
                // depth.
                frame_id++;
                const uint32_t fid = frame_id;

                const uint8_t* cdata = nullptr; uint32_t csize = 0;
                const uint8_t* ddata = nullptr; uint32_t dsize = 0;
                if (colorFrame) {
                    cdata = static_cast<const uint8_t*>(colorFrame->data());
                    csize = colorFrame->dataSize();
                }
                if (depthFrame && depthFrame->format() == OB_FORMAT_Y16) {
                    ddata = static_cast<const uint8_t*>(depthFrame->data());
                    dsize = depthFrame->dataSize();
                }

                // Mode switch now pending: snapshot this final frame into the
                // freeze-frame cache and stop this pipeline. The cache is only
                // refreshed at switch time — no per-frame copy during streaming.
                if (requested_mode.load() != current_mode) {
                    std::lock_guard<std::mutex> lock(cache_mutex);
                    cached_mode = current_mode;
                    if (colorFrame) cached_color.assign(cdata, cdata + csize);
                    if (depthFrame) cached_depth.assign(ddata, ddata + dsize);
                    break;
                }

                // Hand each stream to its own ring independently. Produce is
                // non-blocking: a full ring drops the oldest frame, so the
                // camera never waits — and color is never queued behind the
                // larger, slower depth stream.
                if (colorFrame && csize > 0)
                    color_ring.produce(cdata, csize, fid, (uint8_t)current_mode);
                if (depthFrame && dsize > 0)
                    depth_ring.produce(ddata, dsize, fid, (uint8_t)current_mode);

                // Lite cache refresh (see CACHE_REFRESH_FRAMES above).
                if (++cache_refresh_counter >= CACHE_REFRESH_FRAMES) {
                    cache_refresh_counter = 0;
                    std::lock_guard<std::mutex> lock(cache_mutex);
                    cached_mode = current_mode;
                    if (colorFrame) cached_color.assign(cdata, cdata + csize);
                    if (depthFrame) cached_depth.assign(ddata, ddata + dsize);
                }

                frames_captured++;
                // Capture-fps diagnostic (every 1 second)
                auto now = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double>(now - cam_fps_start).count();
                if (elapsed >= 1.0) {
                    std::cout << "[CAM] capture_fps=" << frames_captured;
                    // Camera frame-cadence jitter: avg + p95 of interval between
                    // consecutive waitForFrameset returns.
                    if (frame_intervals.size() >= 30) {
                        std::vector<float> sorted = frame_intervals;
                        std::sort(sorted.begin(), sorted.end());
                        float sum = 0.0f;
                        for (float v : sorted) sum += v;
                        std::cout << " cam_interval_avg=" << (sum / sorted.size())
                                  << " cam_interval_p95=" << sorted[size_t(sorted.size() * 0.95)];
                    }
                    std::cout << std::endl;
                    frames_captured = 0;
                    cam_fps_start = now;
                }
            }

            // switch the mode
            is_switching.store(true);   // start sending cached frames
            color_pipe.stop();
            depth_pipe.stop();
            std::cout << "[CAM] Pipelines stopped (mode switch)" << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "[CAM ERROR] " << e.what() << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // Wake any sender thread still blocked on a ring so it can observe shutdown.
    {
        std::lock_guard<std::mutex> lk(color_ring.mtx);
        color_ring.consume_cv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(depth_ring.mtx);
        depth_ring.consume_cv.notify_all();
    }
    frz_thread.join();
    col_net.join();
    dep_net.join();
    close(sockfd);
}

int main() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGINT);
    sigprocmask(SIG_BLOCK, &set, nullptr);

    std::thread cam_thread(camera_thread);
    std::thread cmd_thread(command_thread);

    std::cout << "Adaptive stream server running. Waiting for SIGTERM/SIGINT to stop..." << std::endl;

    int sig;
    sigwait(&set, &sig);
    std::cout << "Received signal " << sig << ", shutting down..." << std::endl;
    keep_running = false;
    // Wake any sender thread blocked on a ring so it can observe the shutdown
    // immediately (their wait predicates include keep_running).
    {
        std::lock_guard<std::mutex> lk(color_ring.mtx);
        color_ring.consume_cv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(depth_ring.mtx);
        depth_ring.consume_cv.notify_all();
    }

    cam_thread.join();
    cmd_thread.join();
    return 0;
}