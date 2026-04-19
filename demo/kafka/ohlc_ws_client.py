#!/usr/bin/env python3
"""
Minimal stdlib-only WebSocket client for the Kafka demo OHLC stream.

Usage:
  python3 demo/kafka/ohlc_ws_client.py [--host 127.0.0.1] [--port 8766]
"""

import argparse
import base64
import hashlib
import json
import os
import socket
import struct
from datetime import datetime, timezone


def _make_handshake(host: str, port: int) -> tuple[bytes, str]:
    key_bytes = os.urandom(16)
    key_b64 = base64.b64encode(key_bytes).decode()
    accept = base64.b64encode(
        hashlib.sha1((key_b64 + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
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
    out = bytes(buf[:n])
    del buf[:n]
    return out


def _recv_frame(sock: socket.socket, buf: bytearray) -> tuple[int, bytes]:
    header = _recv_exactly(sock, buf, 2)
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", _recv_exactly(sock, buf, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", _recv_exactly(sock, buf, 8))[0]
    payload = _recv_exactly(sock, buf, length)
    return opcode, payload


def ws_connect(host: str, port: int) -> tuple[socket.socket, bytearray]:
    sock = socket.create_connection((host, port))
    request, expected = _make_handshake(host, port)
    sock.sendall(request)

    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Server closed during handshake")
        response += chunk

    sep = response.index(b"\r\n\r\n")
    headers = response[:sep].decode(errors="replace")
    leftover = bytearray(response[sep + 4 :])
    if "101 Switching Protocols" not in headers:
        raise ConnectionError(headers)
    if expected not in headers:
        raise ConnectionError("Sec-WebSocket-Accept mismatch")
    return sock, leftover


def fmt_ns(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%H:%M:%S")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka demo websocket OHLC client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()

    print(f"Connecting to ws://{args.host}:{args.port}")
    sock, buf = ws_connect(args.host, args.port)
    print("Connected. Waiting for OHLC bars...\n")
    print(f"{'Time':<10} {'Symbol':<8} {'Open':>9} {'High':>9} {'Low':>9} {'Close':>9}")
    print("-" * 60)

    try:
        while True:
            opcode, payload = _recv_frame(sock, buf)
            if opcode == 0x8:
                print("\nServer closed connection.")
                break
            if opcode == 0x9:
                sock.sendall(bytes([0x8A, len(payload)]) + payload)
                continue
            if opcode in (0x1, 0x0):
                msg = json.loads(payload.decode())
                print(
                    f"{fmt_ns(int(msg.get('ts', 0))):<10} {msg.get('symbol', '?'):<8} "
                    f"{float(msg.get('open', 0.0)):>9.2f} {float(msg.get('high', 0.0)):>9.2f} "
                    f"{float(msg.get('low', 0.0)):>9.2f} {float(msg.get('close', 0.0)):>9.2f}"
                )
    finally:
        try:
            sock.sendall(bytes([0x88, 0x00]))
        except OSError:
            pass
        sock.close()


if __name__ == "__main__":
    main()
