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

// ============================================================
// CONFIGURATION
// ============================================================
#define HOST_IP             "192.168.0.249" // ip of target computer
#define UDP_PORT            9999
#define COLOR_WIDTH         1920
#define COLOR_HEIGHT        1080
#define COLOR_FPS           30
#define DEPTH_WIDTH         640
#define DEPTH_HEIGHT        576
#define DEPTH_FPS           30
#define MAX_CHUNK_SIZE      60000
// ============================================================

// Stream type identifiers
#define STREAM_COLOR  0
#define STREAM_DEPTH  1

#pragma pack(push, 1)
struct PacketHeader {
    uint32_t frame_id;       // Shared ID for color+depth sync
    uint16_t chunk_index;
    uint16_t total_chunks;
    uint32_t data_size;      // Payload bytes in this chunk
    uint8_t  stream_type;    // 0=COLOR_MJPEG, 1=DEPTH_Y16
    uint8_t  reserved[3];    // Alignment padding
};
#pragma pack(pop)
// Header size: 16 bytes

std::atomic<bool> keep_running(true);

void send_frame_chunks(int sockfd, const sockaddr_in& dest,
                       const uint8_t* data, uint32_t dataSize,
                       uint32_t frame_id, uint8_t stream_type) {

    uint16_t total_chunks = (dataSize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;
    size_t offset = 0;
    uint16_t chunk_idx = 0;

    while (offset < dataSize) {
        size_t chunk_len = std::min((size_t)MAX_CHUNK_SIZE, (size_t)(dataSize - offset));

        PacketHeader hdr;
        hdr.frame_id = htonl(frame_id);
        hdr.chunk_index = htons(chunk_idx);
        hdr.total_chunks = htons(total_chunks);
        hdr.data_size = htonl((uint32_t)chunk_len);
        hdr.stream_type = stream_type;
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

void camera_thread() {
    try {
        ob::Pipeline pipe;
        auto device = pipe.getDevice();
        std::cout << "[CAM] Device: " << device->getDeviceInfo()->name() << std::endl;

        std::shared_ptr<ob::StreamProfile> colorProfile = nullptr;
        std::shared_ptr<ob::StreamProfile> depthProfile = nullptr;
        auto sensorList = device->getSensorList();
        
        // === find matching color profiles ===
        for (uint32_t i = 0; i < sensorList->count(); i++) {
            auto sensor = sensorList->getSensor(i);
            auto profiles = sensor->getStreamProfileList();

            if (sensor->getType() == OB_SENSOR_COLOR) {
                for (uint32_t j = 0; j < profiles->count(); j++) {
                    auto p = profiles->getProfile(j);
                    auto vp = p->as<ob::VideoStreamProfile>();
                    if (vp && vp->width() == COLOR_WIDTH && vp->height() == COLOR_HEIGHT
                        && vp->fps() == COLOR_FPS && vp->format() == OB_FORMAT_MJPEG) {
                        colorProfile = p;
                        std::cout << "[CAM] Color: " << COLOR_WIDTH << "x" << COLOR_HEIGHT
                                  << " @" << COLOR_FPS << "fps MJPEG" << std::endl;
                        break;
                    }
                }
            }

            if (sensor->getType() == OB_SENSOR_DEPTH) {
                for (uint32_t j = 0; j < profiles->count(); j++) {
                    auto p = profiles->getProfile(j);
                    auto vp = p->as<ob::VideoStreamProfile>();
                    if (vp && vp->width() == DEPTH_WIDTH && vp->height() == DEPTH_HEIGHT
                        && vp->fps() == DEPTH_FPS && vp->format() == OB_FORMAT_Y16) {
                        depthProfile = p;
                        std::cout << "[CAM] Depth: " << DEPTH_WIDTH << "x" << DEPTH_HEIGHT
                                  << " @" << DEPTH_FPS << "fps Y16" << std::endl;
                        break;
                    }
                }
            }
        }

        if (!colorProfile) { std::cerr << "[CAM] Color profile not found!" << std::endl; return; }
        if (!depthProfile) { std::cerr << "[CAM] Depth profile not found!" << std::endl; return; }

        // === Configure Pipeline: BOTH streams + synchronized ===
        auto config = std::make_shared<ob::Config>();
        config->enableStream(colorProfile);
        config->enableStream(depthProfile);
        // Synchronized delivery: wait for BOTH color+depth before returning
        config->setFrameAggregateOutputMode(OB_FRAME_AGGREGATE_OUTPUT_ALL_TYPE_FRAME_REQUIRE);

        // === Setup UDP Socket ===
        int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) { std::cerr << "[NET] Socket creation failed!" << std::endl; return; }

        // Increase send buffer for burst transmission
        int sndbuf = 4 * 1024 * 1024;
        setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

        sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(UDP_PORT);
        inet_pton(AF_INET, HOST_IP, &dest.sin_addr);

        pipe.start(config);
        std::cout << "[CAM] Dual-stream started → " << HOST_IP << ":" << UDP_PORT << std::endl;
        std::cout << "========================================" << std::endl;

        uint32_t frame_id = 0;
        // === FPS Benchmark ===
        uint32_t frames_sent = 0;
        auto fps_start = std::chrono::steady_clock::now();
        uint32_t total_frames_sent = 0;
        uint64_t total_bytes_sent = 0;

        while (keep_running) {
            auto frameSet = pipe.waitForFrameset(100);
            if (!frameSet) continue;

            auto colorFrame = frameSet->colorFrame();
            auto depthFrame = frameSet->depthFrame();

            frame_id++;

            // Send COLOR
            if (colorFrame && colorFrame->format() == OB_FORMAT_MJPEG) {
                const uint8_t* data = static_cast<const uint8_t*>(colorFrame->data());
                uint32_t size = colorFrame->dataSize();
                send_frame_chunks(sockfd, dest, data, size, frame_id, STREAM_COLOR);
                total_bytes_sent += size;  // ✅ Track bytes
            }

            // Send DEPTH
            if (depthFrame && depthFrame->format() == OB_FORMAT_Y16) {
                const uint8_t* data = static_cast<const uint8_t*>(depthFrame->data());
                uint32_t size = depthFrame->dataSize();
                send_frame_chunks(sockfd, dest, data, size, frame_id, STREAM_DEPTH);
                total_bytes_sent += size;  // ✅ Track bytes
            }
            
            // Performance checks
            frames_sent++;
            total_frames_sent++;
            auto now = std::chrono::steady_clock::now();
            double elapsed_sec = std::chrono::duration<double>(now - fps_start).count();

            if (elapsed_sec >= 1.0) {
                double send_fps = frames_sent / elapsed_sec;
                double mbps = (total_bytes_sent * 8.0) / (elapsed_sec * 1'000'000.0);
                
                std::cout << "[BENCH] Send FPS: " << send_fps 
                        << " | Total sent: " << total_frames_sent << " frames"
                        << " | Bandwidth: " << mbps << " Mbps"
                        << std::endl;
                
                // Reset per-second counters
                frames_sent = 0;
                total_bytes_sent = 0;
                fps_start = now;
            }
            // Periodic status print
            if (frame_id % 100 == 0) {
                std::cout << "[CAM] Sent " << frame_id << " synchronized frame pairs" << std::endl;
            }
        }

        pipe.stop();
        close(sockfd);
    } catch (const std::exception& e) {
        std::cerr << "[CAM ERROR] " << e.what() << std::endl;
    }
}

int main() {
    std::thread cam_thread(camera_thread);
    std::cout << "Press Enter to stop..." << std::endl;
    std::cin.get();
    keep_running = false;
    cam_thread.join();
    return 0;
}