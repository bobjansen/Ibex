#!/usr/bin/env python3
"""
ws_client.py — minimal WebSocket client for the Ibex market stream demo.

Connects to the WebSocket server started by market_stream_ws.ibex and prints
each OHLC bar as it arrives.  Uses only the Python standard library (no
third-party websocket packages required).

Usage:
    python3 ws_client.py [--host HOST] [--port PORT]

Defaults:
    --host  127.0.0.1
    --port  8765
"""

import argparse
import base64
import hashlib
import os
import socket
import struct
import json
from datetime import datetime, timezone


# ─── Minimal WebSocket client (stdlib only) ────────────────────────────────────

def _make_handshake(host: str, port: int) -> tuple[bytes, str]:
    """Return (HTTP upgrade request bytes, expected Sec-WebSocket-Accept value)."""
    key_bytes = os.urandom(16)
    key_b64 = base64.b64encode(key_bytes).decode()
    magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    accept = base64.b64encode(
        hashlib.sha1((key_b64 + magic).encode()).digest()
    ).decode()
    request = (
        f"GET /ibex HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key_b64}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    return request.encode(), accept


def _recv_exactly(sock: socket.socket, buf: bytearray, n: int) -> bytes:
    while len(buf) < n:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed")
        buf.extend(chunk)
    result = bytes(buf[:n])
    del buf[:n]
    return result


def _recv_ws_frame(sock: socket.socket, buf: bytearray) -> tuple[int, bytes]:
    """Receive one WebSocket frame; return (opcode, payload)."""
    header = _recv_exactly(sock, buf, 2)
    opcode = header[0] & 0x0F
    payload_len = header[1] & 0x7F
    if payload_len == 126:
        payload_len = struct.unpack(">H", _recv_exactly(sock, buf, 2))[0]
    elif payload_len == 127:
        payload_len = struct.unpack(">Q", _recv_exactly(sock, buf, 8))[0]
    payload = _recv_exactly(sock, buf, payload_len)
    return opcode, payload


def ws_connect(host: str, port: int) -> tuple[socket.socket, bytearray]:
    """Open a WebSocket connection; return (socket, leftover_buffer).

    Any bytes that arrive after the HTTP 101 headers (e.g. the first
    WebSocket frame bundled in the same TCP segment) are returned in the
    bytearray so callers can pass it to _recv_ws_frame without data loss.
    """
    sock = socket.create_connection((host, port))
    request, expected_accept = _make_handshake(host, port)
    sock.sendall(request)

    # Read until end of HTTP headers; preserve any trailing WS frame bytes.
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Server closed connection during handshake")
        response += chunk

    sep = response.index(b"\r\n\r\n")
    headers = response[:sep].decode(errors="replace")
    leftover = bytearray(response[sep + 4:])

    if "101 Switching Protocols" not in headers:
        raise ConnectionError(f"WebSocket upgrade failed:\n{headers}")
    if expected_accept not in headers:
        raise ConnectionError("Sec-WebSocket-Accept mismatch")

    return sock, leftover


# ─── Pretty-print helpers ──────────────────────────────────────────────────────

def ns_to_dt(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Ibex WebSocket bar receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    print(f"Connecting to ws://{args.host}:{args.port} ...")
    sock, buf = ws_connect(args.host, args.port)
    print("Connected.  Waiting for OHLC bars ...\n")
    print(f"{'Time':>25}  {'Open':>8}  {'High':>8}  {'Low':>8}  {'Close':>8}")
    print("-" * 65)

    try:
        while True:
            opcode, payload = _recv_ws_frame(sock, buf)
            if opcode == 0x8:   # Close
                print("\nServer closed connection.")
                break
            if opcode == 0x9:   # Ping — send Pong
                pong = bytes([0x8A, len(payload)]) + payload
                sock.sendall(pong)
                continue
            if opcode in (0x1, 0x0):  # Text / continuation
                try:
                    bar = json.loads(payload.decode())
                    ts_str = ns_to_dt(bar["ts"]) if "ts" in bar else "?"
                    print(f"{ts_str:>25}  {bar.get('open', '?'):>8.2f}  "
                          f"{bar.get('high', '?'):>8.2f}  {bar.get('low', '?'):>8.2f}  "
                          f"{bar.get('close', '?'):>8.2f}")
                except (json.JSONDecodeError, KeyError) as exc:
                    print(f"[bad message: {exc}] {payload!r}")
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        # Send WebSocket close frame
        try:
            sock.sendall(bytes([0x88, 0x00]))
        except OSError:
            pass
        sock.close()


if __name__ == "__main__":
    main()
