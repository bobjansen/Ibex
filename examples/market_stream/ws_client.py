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
    --port  8080
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


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed")
        buf += chunk
    return buf


def _recv_ws_frame(sock: socket.socket) -> tuple[int, bytes]:
    """Receive one WebSocket frame; return (opcode, payload)."""
    header = _recv_exactly(sock, 2)
    opcode = header[0] & 0x0F
    payload_len = header[1] & 0x7F
    if payload_len == 126:
        payload_len = struct.unpack(">H", _recv_exactly(sock, 2))[0]
    elif payload_len == 127:
        payload_len = struct.unpack(">Q", _recv_exactly(sock, 8))[0]
    payload = _recv_exactly(sock, payload_len)
    return opcode, payload


def ws_connect(host: str, port: int) -> socket.socket:
    """Open a WebSocket connection; return the connected socket."""
    sock = socket.create_connection((host, port))
    request, expected_accept = _make_handshake(host, port)
    sock.sendall(request)

    # Read HTTP response headers
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Server closed connection during handshake")
        response += chunk

    response_str = response.decode(errors="replace")
    if "101 Switching Protocols" not in response_str:
        raise ConnectionError(f"WebSocket upgrade failed:\n{response_str}")
    if expected_accept not in response_str:
        raise ConnectionError("Sec-WebSocket-Accept mismatch")

    return sock


# ─── Pretty-print helpers ──────────────────────────────────────────────────────

def ns_to_dt(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Ibex WebSocket bar receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    print(f"Connecting to ws://{args.host}:{args.port} ...")
    sock = ws_connect(args.host, args.port)
    print("Connected.  Waiting for OHLC bars ...\n")
    print(f"{'Time':>25}  {'Open':>8}  {'High':>8}  {'Low':>8}  {'Close':>8}")
    print("-" * 65)

    try:
        while True:
            opcode, payload = _recv_ws_frame(sock)
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
