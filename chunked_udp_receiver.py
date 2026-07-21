import socket
import struct
import cv2
import numpy as np
import time
import threading
import queue

UDP_PORT = 9999
# CRITICAL: Max queue size of 2. If AI is slow, drop OLD frames to keep latency low!
MAX_QUEUE_SIZE = 2 

frame_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
keep_running = True

def network_receiver_thread():
    """Thread 1: Solely dedicated to catching UDP packets and reassembling frames."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    # Massive 4MB buffer to absorb Wi-Fi micro-bursts
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024) 
    
    frame_buffers = {}
    
    while keep_running:
        try:
            data, addr = sock.recvfrom(65535)
        except Exception:
            break
            
        if len(data) < 12: continue
        
        # Unpack 12-byte header
        frame_id, chunk_idx, total_chunks, chunk_size = struct.unpack("!IHHI", data[:12])
        payload = data[12:]
        
        if frame_id not in frame_buffers:
            frame_buffers[frame_id] = [None] * total_chunks
            
        if chunk_idx < total_chunks:
            frame_buffers[frame_id][chunk_idx] = payload
            
        # Check if frame is fully assembled
        if frame_buffers[frame_id] and all(c is not None for c in frame_buffers[frame_id]):
            jpeg_bytes = b"".join(frame_buffers[frame_id])
            del frame_buffers[frame_id] # Free RAM immediately
            
            # Put into queue. If queue is full, drop the OLDEST frame!
            if frame_queue.full():
                try:
                    frame_queue.get_nowait() 
                except queue.Empty:
                    pass
            frame_queue.put(jpeg_bytes)
            
        # Purge incomplete frames (Wi-Fi drops) to prevent RAM leaks
        if len(frame_buffers) > 15:
            oldest_keys = sorted(frame_buffers.keys())[:-5]
            for k in oldest_keys: 
                del frame_buffers[k]

def main():
    """Thread 2: Decoding and AI Inference."""
    # Start the network thread in the background
    threading.Thread(target=network_receiver_thread, daemon=True).start()
    
    frame_count = 0
    start_time = time.time()
    print("[HOST] Multi-threaded receiver started. Waiting for 1080p stream...")
    
    while True:
        try:
            # Block until a fully assembled JPEG is ready
            jpeg_bytes = frame_queue.get(timeout=1.0)
        except queue.Empty:
            continue
            
        # Decode 1080p MJPEG to OpenCV Mat
        nparr = np.frombuffer(jpeg_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if frame is not None:
            frame_count += 1
            
            # ==========================================
            # TODO: Pass 'frame' to YOLO TensorRT here!
            # ==========================================

            cv2.imshow("Femto Mega 1080p (Low Latency)", frame)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
                
            if frame_count % 30 == 0:
                elapsed = time.time() - start_time
                print(f"[HOST] True Decode FPS: {frame_count / elapsed:.2f}")

    global keep_running
    keep_running = False
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()