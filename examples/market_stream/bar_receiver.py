#!/usr/bin/env python3
"""
bar_receiver.py — prints OHLC bars forwarded by the Ibex UDP stream demo.

Listens on localhost:9002 for JSON bar datagrams produced by the udp_send plugin:

    {"ts":<ns_since_epoch>,"open":<float>,"high":<float>,"low":<float>,"close":<float>}

Usage:
    python3 bar_receiver.py [--host HOST] [--port PORT]
"""

import argparse
import json
import socket
from datetime import datetime, timezone


def ns_to_dt(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    parser = argparse.ArgumentParser(description="Ibex UDP bar receiver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9002)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    print(f"Listening for OHLC bars on {args.host}:{args.port} ...")
    print(f"{'Time':>25}  {'Open':>8}  {'High':>8}  {'Low':>8}  {'Close':>8}")
    print("-" * 65)

    while True:
        data, _ = sock.recvfrom(4096)
        try:
            bar = json.loads(data.decode())
            ts_str = ns_to_dt(bar["ts"]) if "ts" in bar else "?"
            print(f"{ts_str:>25}  {bar.get('open', '?'):>8.2f}  "
                  f"{bar.get('high', '?'):>8.2f}  {bar.get('low', '?'):>8.2f}  "
                  f"{bar.get('close', '?'):>8.2f}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[bad datagram: {e}] {data!r}")


if __name__ == "__main__":
    main()
