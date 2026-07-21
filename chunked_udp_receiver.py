import socket
import struct
import cv2
import numpy as np
import time
import threading
import queue

# ============================================================
# CONFIGURATION
# ============================================================
UDP_PORT = 9999
DEPTH_WIDTH = 640
DEPTH_HEIGHT = 576
MAX_QUEUE_SIZE = 3  # Keep only freshest frames for low latency
HEADER_SIZE = 16    # Must match C++ PacketHeader struct
# ============================================================

STREAM_COLOR = 0
STREAM_DEPTH = 1

frame_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
keep_running = True


def network_receiver_thread():
    """Thread 1: Catch UDP chunks, reassemble color+depth, pair by frame_id."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)  # 8MB buffer

    # Reassembly buffers: {frame_id: {stream_type: [chunks]}}
    buffers = {}
    # Track metadata for each frame
    frame_meta = {}  # {frame_id: {"color_total": N, "depth_total": M}}

    while keep_running:
        try:
            data, addr = sock.recvfrom(65535)
        except Exception:
            break

        if len(data) < HEADER_SIZE:
            continue

        # Unpack 16-byte header (network byte order)
        frame_id, chunk_idx, total_chunks, data_size, stream_type = \
            struct.unpack("!IHHIB", data[:13])
        # Skip 3 reserved bytes
        payload = data[HEADER_SIZE:]

        # Initialize buffer structure for this frame
        if frame_id not in buffers:
            buffers[frame_id] = {STREAM_COLOR: None, STREAM_DEPTH: None}
            frame_meta[frame_id] = {}

        # Initialize chunk list for this stream type
        if buffers[frame_id][stream_type] is None:
            buffers[frame_id][stream_type] = [None] * total_chunks
            frame_meta[frame_id][stream_type] = total_chunks

        # Store chunk
        if chunk_idx < total_chunks:
            buffers[frame_id][stream_type][chunk_idx] = payload

        # Check if BOTH streams are fully assembled for this frame
        color_done = (buffers[frame_id][STREAM_COLOR] is not None and
                      all(c is not None for c in buffers[frame_id][STREAM_COLOR]))
        depth_done = (buffers[frame_id][STREAM_DEPTH] is not None and
                      all(c is not None for c in buffers[frame_id][STREAM_DEPTH]))

        if color_done and depth_done:
            color_jpeg = b"".join(buffers[frame_id][STREAM_COLOR])
            depth_raw = b"".join(buffers[frame_id][STREAM_DEPTH])
            del buffers[frame_id]
            del frame_meta[frame_id]

            # Enqueue paired frame (drop oldest if queue full)
            if frame_queue.full():
                try:
                    frame_queue.get_nowait()
                except queue.Empty:
                    pass
            frame_queue.put((frame_id, color_jpeg, depth_raw))

        # Purge stale incomplete frames (prevent RAM leak from dropped packets)
        if len(buffers) > 20:
            oldest_keys = sorted(buffers.keys())[:-10]
            for k in oldest_keys:
                del buffers[k]
                if k in frame_meta:
                    del frame_meta[k]


def main():
    """Thread 2: Decode and process synchronized color+depth pairs."""
    threading.Thread(target=network_receiver_thread, daemon=True).start()

    frame_count = 0
    start_time = time.perf_counter()
    print(f"[HOST] Dual-stream receiver listening on UDP {UDP_PORT}...")
    print(f"[HOST] Expecting: 1080p30 MJPEG + 640x576@30 Y16 depth")
    print("=" * 50)

    while True:
        try:
            frame_id, color_jpeg, depth_raw = frame_queue.get(timeout=2.0)
        except queue.Empty:
            print("[HOST] ⚠️ No frames received in 2s. Check Jetson Nano.")
            continue

        # === Decode Color (MJPEG → BGR) ===
        nparr_color = np.frombuffer(color_jpeg, np.uint8)
        color_frame = cv2.imdecode(nparr_color, cv2.IMREAD_COLOR)

        # === Decode Depth (raw Y16 → uint16 array) ===
        depth_array = np.frombuffer(depth_raw, dtype=np.uint16).reshape(
            (DEPTH_HEIGHT, DEPTH_WIDTH))

        # === Normalize depth for visualization (0-4000mm range) ===
        depth_vis = cv2.normalize(depth_array, None, 0, 255, cv2.NORM_MINMAX)
        depth_vis = depth_vis.astype(np.uint8)
        depth_colormap = cv2.applyColorMap(depth_vis, cv2.COLORMAP_JET)

        if color_frame is not None:
            frame_count += 1

            # ==========================================
            # TODO: Your YOLO + MediaPipe pipeline here
            # color_frame: 1920x1080 BGR (ready for inference)
            # depth_array: 640x576 uint16 (millimeters)
            #
            # For 3D hand tracking:
            #   1. Run MediaPipe on color_frame → 2D landmarks
            #   2. Map landmark (x,y) to depth_array coordinates
            #   3. Read depth_array[y,x] → Z distance in mm
            # ==========================================

            # Display side-by-side
            depth_resized = cv2.resize(depth_colormap, (640, 360))
            color_resized = cv2.resize(color_frame, (640, 360))
            combined = np.hstack((color_resized, depth_resized))

            cv2.imshow("Femto Mega: Color + Depth", combined)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

            if frame_count % 30 == 0:
                elapsed = time.perf_counter() - start_time
                fps = frame_count / elapsed
                # reset fps to average of this batch
                frame_count = 0
                start_time = time.perf_counter()
                print(f"[HOST] Paired FPS: {fps:.2f} | "
                      f"Color: {color_frame.shape} | "
                      f"Depth: {depth_array.shape} | "
                      f"Depth range: {depth_array.min()}-{depth_array.max()}mm")

    global keep_running
    keep_running = False
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()