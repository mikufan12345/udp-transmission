"""Production-grade UDP receiver for the Orbbec MJPEG + Y16 depth stream.

Ports the decoupled reassembly core from the air-mouse production receiver
(Artificial-Assistance/backend/air_mouse/camera_pipeline.py _UdpReceiverThread)
into a self-contained script.

Key property: colour is released the instant its own chunks complete, with the
freshest complete depth attached (depth trails by <=1 frame). There is no
wait-for-both pairing, so colour is never blocked on depth.
"""

from __future__ import annotations

import logging
import queue
import socket
import struct
import threading
import time

_log = logging.getLogger(__name__)

# Protocol constants (must match the C++ sender PacketHeader)
_HEADER_SIZE = 16  # frame_id(4) + chunk_idx(2) + total_chunks(2) + data_size(4) + stream_type(1) + reserved(3)
_HEADER_STRUCT = struct.Struct("!IHHIB")  # fields before the 3 reserved bytes
_STREAM_COLOR = 0
_STREAM_DEPTH = 1
_DEPTH_WIDTH = 640
_DEPTH_HEIGHT = 576
_MAX_QUEUE_SIZE = 3  # keep only the freshest frames (latest-frame-wins)


class _DecoupledUdpReceiver:
    """Daemon thread that receives UDP chunks and reassembles color+depth.

    Colour frames are released as soon as they complete, carrying the freshest
    complete depth map captured so far. Depth is reassembled into a single slot
    (only the latest matters) and may trail colour by <=1 frame.
    """

    def __init__(self, port: int) -> None:
        self.port = port
        self.frame_queue: queue.Queue[tuple[int, bytes, bytes]] = queue.Queue(
            maxsize=_MAX_QUEUE_SIZE
        )
        self._keep_running = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._keep_running.set()
        self._thread = threading.Thread(
            target=self._run, name="udp-receiver", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._keep_running.clear()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
        sock.settimeout(1.0)

        _log.info("UDP receiver listening on port %d", self.port)

        # Reassembly state: {frame_id: [received_chunk_count, [chunks]]}. The
        # completion check is O(1) (count == total) instead of an O(n) all() scan
        # over the chunk list.
        buffers: dict[int, list[object]] = {}
        # Freshest fully-complete depth (raw Y16 bytes), attached to each colour
        # frame. Depth is decoupled from colour so colour is released
        # immediately; it may trail by <=1 frame.
        latest_depth_raw = b""
        latest_depth_fid = -1
        # Single-slot depth reassembly: [frame_id, received_chunk_count, chunks]
        # (only the latest completed depth matters, so no dict per frame_id).
        d_buffer: list[object] = [-1, 0, None]

        while self._keep_running.is_set():
            try:
                data, _addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break

            if len(data) < _HEADER_SIZE:
                continue

            # Unpack 16-byte header (network byte order)
            frame_id, chunk_idx, total_chunks, _data_size, stream_type = \
                _HEADER_STRUCT.unpack_from(data, 0)
            payload = data[_HEADER_SIZE:]

            if stream_type == _STREAM_COLOR:
                # Colour drives the pipeline and is released as soon as it
                # completes; the most recent complete depth is attached to each
                # colour packet (it may trail by ~1 frame).
                if frame_id not in buffers:
                    buffers[frame_id] = [0, [None] * total_chunks]
                count, chunk_list = buffers[frame_id]
                if chunk_idx < total_chunks and chunk_list[chunk_idx] is None:
                    chunk_list[chunk_idx] = payload
                    count += 1
                    buffers[frame_id][0] = count
                if count == total_chunks:
                    color_jpeg = b"".join(chunk_list)
                    del buffers[frame_id]
                    # Drop oldest if queue full (latest-frame-wins)
                    if self.frame_queue.full():
                        try:
                            self.frame_queue.get_nowait()
                        except queue.Empty:
                            pass
                    self.frame_queue.put((frame_id, color_jpeg, latest_depth_raw))
            else:
                # Depth stream (or any other stream): reassemble, keep only the
                # freshest complete depth for attachment to future colour frames.
                if d_buffer[0] != frame_id:
                    d_buffer[0] = frame_id
                    d_buffer[1] = 0
                    d_buffer[2] = [None] * total_chunks
                if chunk_idx < total_chunks and d_buffer[2][chunk_idx] is None:
                    d_buffer[2][chunk_idx] = payload
                    d_buffer[1] += 1
                if d_buffer[1] == total_chunks:
                    # Only forward a newer/equal frame so a late, stale depth
                    # (e.g. after a sender restart) never regresses the freshest
                    # map.
                    if frame_id >= latest_depth_fid:
                        latest_depth_raw = b"".join(d_buffer[2])
                        latest_depth_fid = frame_id
                    # Mark consumed so a stale frame_id won't overwrite the
                    # freshest depth and the next frame starts from scratch.
                    d_buffer[0] = -1
                    d_buffer[1] = 0
                    d_buffer[2] = None

            # Purge stale incomplete frames (prevent RAM leak from dropped packets)
            if len(buffers) > 20:
                oldest_keys = sorted(buffers.keys())[:-10]
                for k in oldest_keys:
                    del buffers[k]

        sock.close()
        _log.info("UDP receiver stopped")


def main() -> None:
    """Happy-path demo: decode and display paired colour+depth frames."""
    import argparse

    import cv2
    import numpy as np

    parser = argparse.ArgumentParser(description="Production decoupled UDP receiver demo")
    parser.add_argument("--port", type=int, default=9999)
    args = parser.parse_args()

    rx = _DecoupledUdpReceiver(args.port)
    rx.start()
    print(f"Listening on UDP {args.port} (decoupled colour+depth)...")

    frame_count = 0
    start_time = time.perf_counter()
    try:
        while True:
            try:
                _frame_id, color_jpeg, depth_raw = rx.frame_queue.get(timeout=2.0)
            except queue.Empty:
                print("No frames received in 2s. Check the Jetson sender.")
                continue

            color_frame = cv2.imdecode(np.frombuffer(color_jpeg, np.uint8), cv2.IMREAD_COLOR)
            if color_frame is None:
                print("Warning: failed to decode MJPEG colour frame")
                continue
            if depth_raw:
                depth_array = np.frombuffer(depth_raw, dtype=np.uint16).reshape(
                    (_DEPTH_HEIGHT, _DEPTH_WIDTH)
                )
            else:
                depth_array = np.zeros((_DEPTH_HEIGHT, _DEPTH_WIDTH), dtype=np.uint16)

            depth_vis = cv2.normalize(depth_array, None, 0, 255, cv2.NORM_MINMAX)
            depth_vis = depth_vis.astype(np.uint8)
            depth_colormap = cv2.applyColorMap(depth_vis, cv2.COLORMAP_JET)

            frame_count += 1
            color_resized = cv2.resize(color_frame, (640, 360))
            combined = np.hstack((color_resized, cv2.resize(depth_colormap, (640, 360))))

            cv2.imshow("Femto Mega: Color + Depth", combined)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

            if frame_count % 30 == 0:
                fps = frame_count / (time.perf_counter() - start_time)
                frame_count = 0
                start_time = time.perf_counter()
                print(f"Paired FPS: {fps:.2f} | Color: {color_frame.shape} | "
                      f"Depth: {depth_array.shape} | "
                      f"Depth range: {depth_array.min()}-{depth_array.max()}mm")
    finally:
        rx.stop()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()