# udp-transmission

Low-latency dual-stream video pipeline for Orbbec Femto Mega cameras. The C++ `udp_server` captures MJPEG color + Y16 depth and streams them as chunked UDP datagrams to a Python `udp_receiver`.

## Quickstart

```bash
# ssh into the femto mega
ssh orb@192.168.0.130               # your ip might be different
```

Make sure that the IP address of the server is correct. And make sure that the gateway is configured correctly.   
If you encounter any problems, check in OrbbecViewer.  

Set up the repository.
```bash
git clone https://github.com/mikufan12345/udp-transmission.git
cd udp-transmission
```

Change configuration as required. Especially note that your IP address might be different. (See: [Configuration](#configuration))  
Then set up the service. Manual setup is not recommended.
```bash
# Building requires Orbbec SDK v2.x, CMake >= 3.10, g++ with C++17, pthread.
# setup script builds automatically.
chmod +x ./setup.sh ./build.sh      # these scripts should have x flag by default, but just to be safe
sudo ./setup.sh
```

## Manual setup

### How to start the service

```bash
cp ./udp-server.service /etc/systemd/system/udp-server.service
systemctl enable udp-server && systemctl start udp-server
```

Manual service control:

```bash
sudo systemctl restart udp-server
# view the live logs
sudo journalctl -u udp-server -f
```

### Build

An automated build script is available.
```bash
chmod +x build.sh setup.sh
./build.sh
```
Output binary: `build/udp_streamer`

## Configuration

### Server (`udp_server.cpp`)

Edit `udp_server.cpp` to change the fallback destination IP, ports, chunk size, and batch size:

```cpp
#define HOST_IP             "192.168.0.249"   // Fallback if no receiver registers
#define STREAM_PORT         9999              // UDP stream port
#define COMMAND_PORT        9998              // TCP command + registration port
#define MAX_CHUNK_SIZE      1400
#define SEND_BATCH          32
#define RING_SLOTS          2
```

- The server must have a **static IP** on the local network (e.g. via DHCP reservation).
- `HOST_IP` is the **fallback** destination used when no receiver has registered.
- When a receiver connects to the TCP command port (9998) and sends `REGISTER`, the
  server captures the receiver's source IP and streams UDP to it instead.
- No DNS, mDNS, or `CAP_NET_RAW` required.

### Receiver (`udp_receiver.py`)

```bash
python udp_receiver.py --host <server-static-ip> [--port 9999] [--command-port 9998]
```

- `--host` (required): the server's static IP address.
- `--port` (default 9999): UDP stream listening port.
- `--command-port` (default 9998): TCP command port for registration.

The receiver connects to the server's TCP command port, registers its address,
and then listens for UDP on `--port`. A background thread maintains the TCP
connection and auto-reconnects if the link drops (e.g. after a DHCP IP change).

Receiver buffer and queue sizes are constants at the top of each Python file.

## Structural deep-dive (AI summary)

### Components

| Component | Language | Role |
|-----------|----------|------|
| `udp_server.cpp` | C++17 | Camera capture, chunking, `sendmmsg` batching, TCP command listener |
| `udp_receiver.py` | Python | Production decoupled receiver (color released immediately, depth attached) |
| `build.sh` | Bash | cmake build + service restart |
| `setup.sh` | Bash | Install systemd service + build |
| `udp-server.service` | systemd | Auto-start `udp_streamer` on boot |

### Architecture

```
Orbbec Camera
    │
    ▼
[udp_server.cpp]               [udp_receiver.py]
  camera_thread()    ──UDP──►  _DecoupledUdpReceiver
    ├─ color_ring (produce/consume)   │
    └─ depth_ring (produce/consume)   └─ frame_queue
  command_thread()  ──TCP──►  CLOSE / FAR mode switch
    ├─ accept() captures receiver IP  ├─ send REGISTER → server learns our IP
    └─ send "OK" confirmation          └─ keepalive + auto-reconnect
  freeze_thread()   ──UDP──►  cached-frame bridge during mode switch
```

#### Packet format (16-byte header)

```
frame_id(4) | chunk_index(2) | total_chunks(2) | data_size(4) | stream_type(1) | reserved(3)
```

- `stream_type = 0` → MJPEG color (port 9999)
- `stream_type = 1` → Y16 depth (port 9999)
- TCP command port: 9998

#### Receiver registration protocol (TCP 9998)

Since receivers typically have dynamic DHCP addresses, the server cannot
hardcode a destination IP. Instead, the receiver registers itself:

1. **Receiver** opens a TCP connection to `server_static_ip:9998`.
2. **Receiver** sends `REGISTER\n`.
3. **Server** captures the receiver's source IP from `accept()` (no DNS needed),
   stores it, and replies `OK\n`.
4. **Server** streams UDP data to the registered IP on port 9999.
5. If the TCP connection drops (e.g. DHCP IP renewal), the receiver
   automatically reconnects and re-registers.

This works with **zero DNS** — the only configuration the receiver needs is
the server's static IP.

### Send mode switch commands

From any TCP client on port 9998:

```bash
echo "FAR" | nc <server-ip> 9998
echo "CLOSE" | nc <server-ip> 9998
```

During the switch, cached frames are re-sent at ~15 fps as a freeze-frame bridge.

### Key design choices

- **`sendmmsg` batching**: 32 datagrams per syscall; a 4K frame (~1500 chunks) costs ~47 syscalls instead of ~1500.
- **Non-blocking rings**: Camera thread never stalls on network backpressure; oldest frame is evicted when full.
- **Decoupled reassembly**: Color is never delayed by depth; depth trails by <=1 frame.
- **Stale depth guard**: Only forward depth with `frame_id >= latest_depth_fid` to prevent regression after sender restart.
- **Auto-recovery**: Watchdog restarts the pipeline if a sensor is silent for >3s.

## Run receivers (host PC)

You need to unblock port 9999 and 9998 on the firewall.

### Production receiver (recommended)

```bash
pip install opencv-python numpy
python udp_receiver.py --port 9999
```

Color is released as soon as it completes, with the freshest available depth attached. Uses a `latest-frame-wins` queue of depth 3.

### Paired receiver

```bash
python chunked_udp_receiver.py
```

Blocks until both color and depth for the same `frame_id` are complete before delivering. Higher latency, useful for benchmarking synchronization.

### Minimal / callback receiver

```bash
python minimal_udp_receiver.py
```

No display loop. Delivers decoded BGR + depth_mm arrays via `on_frame(color_bgr, depth_mm, received_at_ns)` callback. Lowest queue depth (1 frame).
