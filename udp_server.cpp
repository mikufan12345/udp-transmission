#include <libobsensor/ObSensor.hpp>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <vector>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>

// ============================================================
// CONFIGURATION
// ============================================================
#define HOST_IP             "192.168.0.249"
#define STREAM_PORT         9999    // Video stream (out)
#define COMMAND_PORT        9998    // Mode switch commands (in)
#define MAX_CHUNK_SIZE      60000
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
void send_frame_chunks(int sockfd, const sockaddr_in& dest,
                       const uint8_t* data, uint32_t dataSize,
                       uint32_t frame_id, uint8_t stream_type, uint8_t mode) {
    uint16_t total_chunks = (dataSize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;
    size_t offset = 0;
    uint16_t chunk_idx = 0;
    
    while (offset < dataSize) {
        size_t chunk_len = std::min((size_t)MAX_CHUNK_SIZE, (size_t)(dataSize - offset));
        
        PacketHeader hdr;
        // convert data to network packet endianness
        hdr.frame_id = htonl(frame_id);
        hdr.chunk_index = htons(chunk_idx);
        hdr.total_chunks = htons(total_chunks);
        hdr.data_size = htonl((uint32_t)chunk_len);
        hdr.stream_type = stream_type;
        hdr.mode = mode;
        memset(hdr.reserved, 0, sizeof(hdr.reserved));
        
        std::vector<uint8_t> packet(sizeof(PacketHeader) + chunk_len);
        memcpy(packet.data(), &hdr, sizeof(hdr));
        memcpy(packet.data() + sizeof(hdr), data + offset, chunk_len);
        
        sendto(sockfd, packet.data(), packet.size(), 0,
        reinterpret_cast<const struct sockaddr*>(&dest), sizeof(dest));

        offset += chunk_len;
        chunk_idx++;
    }
}

// === Freeze frame sender thread ===
void freeze_thread(int sockfd, const sockaddr_in& dest) {
    while (keep_running) {
        if (is_switching.load()) {
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
void command_thread() {
    int cmd_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (cmd_sock < 0) {
        std::cerr << "[CMD] Socket creation failed!" << std::endl;
        return;
    }
    
    // set options: REUSEADDR (fast rebinding) and SO_RCVTIMEO (200ms timeout)
    int opt = 1;
    setsockopt(cmd_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200000;
    setsockopt(cmd_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

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

    std::cout << "[CMD] Listening on port " << COMMAND_PORT << std::endl;

    char buf[64];
    while (keep_running) {
        ssize_t n = recv(cmd_sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) continue;

        buf[n] = '\0';
        std::string cmd(buf);

        // Trim whitespace/newline
        while (!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r' || cmd.back() == ' '))
            cmd.pop_back();

        if (cmd == "CLOSE" && active_mode.load() != MODE_CLOSE) {
            std::cout << "[CMD] Received: CLOSE → switching to 1080p@30" << std::endl;
            requested_mode.store(MODE_CLOSE);
        } else if (cmd == "FAR" && active_mode.load() != MODE_FAR) {
            std::cout << "[CMD] Received: FAR → switching to 4K@25" << std::endl;
            requested_mode.store(MODE_FAR);
        }
    }
    close(cmd_sock);
}

// === Camera Thread ===
void camera_thread() {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) { std::cerr << "[NET] Socket failed!" << std::endl; return; }

    // make big 4MB buffer
    int sndbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(STREAM_PORT);
    inet_pton(AF_INET, HOST_IP, &dest.sin_addr);

    uint32_t frame_id = 0;
    std::thread frz_thread(freeze_thread, sockfd, dest);

    // FPS benchmark
    uint32_t frames_sent = 0;
    uint64_t total_bytes_sent = 0;
    auto fps_start = std::chrono::steady_clock::now();

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
            ob::Pipeline pipe;
            auto device = pipe.getDevice();

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

            auto config = std::make_shared<ob::Config>();
            config->enableStream(colorProfile);
            config->enableStream(depthProfile);
            // ANY_SITUATION: return a FrameSet as soon as ANY frame is ready,
            // instead of ALL_TYPE_FRAME_REQUIRE which holds the colour frame
            // until its paired depth arrives. This decouples the two streams so
            // colour reaches the wire at its own cadence; depth may trail by up
            // to a frame (fine — the receiver samples depth natively as a scalar).
            config->setFrameAggregateOutputMode(OB_FRAME_AGGREGATE_OUTPUT_ANY_SITUATION);

            pipe.start(config);
            is_switching.store(false);
            std::cout << "[CAM] Streaming: " << cfg.color_w << "x" << cfg.color_h
                      << "@" << cfg.color_fps << " + "
                      << cfg.depth_w << "x" << cfg.depth_h
                      << "@" << cfg.depth_fps
                      << " → " << HOST_IP << ":" << STREAM_PORT << std::endl;

            // Watchdog: track last successful frame time
            auto last_frame_time = std::chrono::steady_clock::now();

            // === Frame loop ===
            while (keep_running && requested_mode.load() == current_mode) {
                auto frame_t0 = std::chrono::steady_clock::now();
                auto frameSet = pipe.waitForFrameset(100);
                
                // Watchdog: no frames for 3 seconds -> break to restart pipeline
                if (!frameSet) {
                    auto now = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_frame_time).count() > 3) {
                        std::cerr << "[CAM] Watchdog: no frames for 3s, breaking to restart pipeline" << std::endl;
                        break;
                    }
                    continue;
                }
                last_frame_time = frame_t0;

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

                // ANY_SITUATION: the FrameSet may carry colour only, depth only,
                // or both. Each stream is sent immediately when it appears under
                // the same pair frame_id; the receiver releases on colour and
                // attaches the freshest complete depth, so a ~1-frame depth
                // trail is fine for the native scalar sampling.
                auto colorFrame = frameSet->colorFrame();
                auto depthFrame = frameSet->depthFrame();
                if (!colorFrame && !depthFrame) {
                    continue;   // nothing to send this frameset
                }
                frame_id++;
                auto startSend = std::chrono::steady_clock::now();
                const uint32_t fid = frame_id;

                if (colorFrame) {
                    const uint8_t* cdata = static_cast<const uint8_t*>(colorFrame->data());
                    const uint32_t csize = colorFrame->dataSize();
                    send_frame_chunks(sockfd, dest, cdata, csize, fid, STREAM_COLOR, current_mode);
                    total_bytes_sent += csize;
                    cached_color.assign(cdata, cdata + csize);
                }
                if (depthFrame && depthFrame->format() == OB_FORMAT_Y16) {
                    const uint8_t* ddata = static_cast<const uint8_t*>(depthFrame->data());
                    const uint32_t dsize = depthFrame->dataSize();
                    send_frame_chunks(sockfd, dest, ddata, dsize, fid, STREAM_DEPTH, current_mode);
                    total_bytes_sent += dsize;
                    cached_depth.assign(ddata, ddata + dsize);
                }

                frames_sent++;
                auto endSend = std::chrono::steady_clock::now();

                // FPS benchmark (every 1 second)
                auto now = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double>(now - fps_start).count();
                if (elapsed >= 1.0) {
                    double fps = frames_sent;
                    double mbps = (total_bytes_sent * 8.0 / 1024 / 1024);
                    std::cout << "[BENCH] "
                              << (current_mode == MODE_FAR ? "FAR(4K@25)" : "CLOSE(1080p@30)")
                              << " | FPS: " << fps
                              << " | Bandwidth: " << mbps << " Mbps"
                              << " | Frames: " << frame_id
                              << " | Time used: " << std::chrono::duration<double>(endSend - startSend).count();
                    // Camera frame-cadence jitter: avg + p95 of interval between
                    // consecutive waitForFrameset returns. Compares directly to
                    // the receiver's on-the-wire "gap p95".
                    if (frame_intervals.size() >= 30) {
                        std::vector<float> sorted = frame_intervals;
                        std::sort(sorted.begin(), sorted.end());
                        float sum = 0.0f;
                        for (float v : sorted) sum += v;
                        float avg = sum / sorted.size();
                        float p95 = sorted[size_t(sorted.size() * 0.95)];
                        std::cout << " | cam_interval_avg=" << avg
                                  << " cam_interval_p95=" << p95;
                    }
                    std::cout << std::endl;
                    frames_sent = 0;
                    total_bytes_sent = 0;
                    fps_start = now;
                }
            }
            
            // switch the mode
            is_switching.store(true);   // start sending cached frames
            // update cache with last frame
            {
                std::lock_guard<std::mutex> lock(cache_mutex);
                cached_mode = current_mode;
            }
            pipe.stop();
            std::cout << "[CAM] Pipeline stopped (mode switch)" << std::endl;

        } catch (const std::exception& e) {
            std::cerr << "[CAM ERROR] " << e.what() << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    frz_thread.join();
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

    cam_thread.join();
    cmd_thread.join();
    return 0;
}