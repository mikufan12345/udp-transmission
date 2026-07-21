#include <libobsensor/ObSensor.hpp>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// ============================================================
// CONFIGURATION
// ============================================================
#define HOST_IP         "192.168.0.249"  // ← Your Windows Host IP
#define UDP_PORT        9999
#define TARGET_WIDTH    3840
#define TARGET_HEIGHT   2160
#define TARGET_FPS      25
#define MAX_CHUNK_SIZE  60000           // Safely under the 65507 UDP limit
// ============================================================

#pragma pack(push, 1)
struct PacketHeader {
    uint32_t frame_id;
    uint16_t chunk_index;
    uint16_t total_chunks;
    uint32_t data_size;
};
#pragma pack(pop)

std::atomic<bool> keep_running(true);

void camera_thread() {
    try {
        ob::Pipeline pipe;
        auto device = pipe.getDevice();
        std::cout << "[CAM] Device: " << device->getDeviceInfo()->name() << std::endl;

        std::shared_ptr<ob::StreamProfile> colorProfile = nullptr;
        auto sensorList = device->getSensorList();
        
        for (uint32_t i = 0; i < sensorList->count(); i++) {
            auto sensor = sensorList->getSensor(i);
            if (sensor->getType() == OB_SENSOR_COLOR) {
                auto profiles = sensor->getStreamProfileList();
                for (uint32_t j = 0; j < profiles->count(); j++) {
                    auto p = profiles->getProfile(j);
                    auto vp = p->as<ob::VideoStreamProfile>();
                    if (vp && vp->width() == TARGET_WIDTH && vp->height() == TARGET_HEIGHT 
                        && vp->fps() == TARGET_FPS && vp->format() == OB_FORMAT_MJPEG) {
                        colorProfile = p;
                        break;
                    }
                }
                if (!colorProfile && profiles->count() > 0) colorProfile = profiles->getProfile(0);
                break;
            }
        }

        if (!colorProfile) { std::cerr << "[CAM] No color profile found!" << std::endl; return; }

        auto config = std::make_shared<ob::Config>();
        config->enableStream(colorProfile);
        config->setFrameAggregateOutputMode(OB_FRAME_AGGREGATE_OUTPUT_COLOR_FRAME_REQUIRE);

        // Setup UDP Socket
        int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        sockaddr_in servaddr{};
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(UDP_PORT);
        inet_pton(AF_INET, HOST_IP, &servaddr.sin_addr);

        pipe.start(config);
        std::cout << "[CAM] Streaming Chunked UDP to " << HOST_IP << ":" << UDP_PORT << "..." << std::endl;

        uint32_t frame_id = 0;

        while (keep_running) {
            auto frameSet = pipe.waitForFrameset(100);
            if (!frameSet) continue;

            auto colorFrame = frameSet->colorFrame();
            if (!colorFrame || colorFrame->format() != OB_FORMAT_MJPEG) continue;

            const uint8_t* data = static_cast<const uint8_t*>(colorFrame->data());
            uint32_t dataSize = colorFrame->dataSize();
            
            frame_id++;
            size_t offset = 0;
            uint16_t chunk_idx = 0;
            uint16_t total_chunks = (dataSize + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;

            // Blast chunks over UDP (Non-blocking, ultra-low latency)
            while (offset < dataSize) {
                size_t chunk_len = std::min((size_t)MAX_CHUNK_SIZE, dataSize - offset);
                
                PacketHeader hdr;
                hdr.frame_id = htonl(frame_id);
                hdr.chunk_index = htons(chunk_idx);
                hdr.total_chunks = htons(total_chunks);
                hdr.data_size = htonl(chunk_len);

                // Pack header + payload into a single buffer to guarantee 1 UDP packet
                std::vector<uint8_t> packet(sizeof(PacketHeader) + chunk_len);
                memcpy(packet.data(), &hdr, sizeof(hdr));
                memcpy(packet.data() + sizeof(hdr), data + offset, chunk_len);

                sendto(sockfd, packet.data(), packet.size(), 0, 
                       reinterpret_cast<struct sockaddr*>(&servaddr), sizeof(servaddr));

                offset += chunk_len;
                chunk_idx++;
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
