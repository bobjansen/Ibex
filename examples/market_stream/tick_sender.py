#!/usr/bin/env python3
"""
tick_sender.py — synthetic market tick generator for the Ibex UDP stream demo.

Sends JSON-encoded tick datagrams to localhost:9001, one per simulated second.
Each datagram follows the wire protocol expected by the udp_recv plugin:

    {"ts":<ns_since_epoch>,"symbol":"AAPL","price":<float>,"volume":<int>}

After sending `--ticks` ticks the script sends an EOF sentinel:

    {"eof":true}

Usage:
    python3 tick_sender.py [--host HOST] [--port PORT] [--ticks N] [--delay SECS]

Defaults:
    --host  127.0.0.1
    --port  9001
    --ticks 300          (5 minutes of 1-second tick data = 5 complete OHLC bars)
    --delay 0.05         (50 ms between datagrams — faster than real-time for demo)
"""

import argparse
import json
import math
import socket
import time


def generate_ticks(n: int, start_ts_ns: int, interval_ns: int):
    """Yield synthetic tick dicts.  Price follows a sine wave around 100."""
    for i in range(n):
        ts = start_ts_ns + i * interval_ns
        # Sawtooth price 95..105 with a gentle sine modulation
        base = 100.0 + 5.0 * math.sin(2 * math.pi * i / 60)
        price = round(base + (i % 7) * 0.1 - 0.3, 2)
        volume = 500 + (i * 37) % 1500
        yield {
            "ts": ts,
            "symbol": "AAPL",
            "price": price,
            "volume": volume,
        }


def main():
    parser = argparse.ArgumentParser(description="Ibex UDP tick sender")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--ticks", type=int, default=300,
                        help="number of ticks to send (default 300 = 5 minutes)")
    parser.add_argument("--delay", type=float, default=0.05,
                        help="seconds between datagrams (default 0.05)")
    args = parser.parse_args()

    # Start timestamps at the most recent round minute so the first bucket
    # closes after 60 ticks (60 seconds of simulated time).
    now_ns = int(time.time()) * 1_000_000_000
    minute_ns = 60 * 1_000_000_000
    start_ns = (now_ns // minute_ns) * minute_ns  # floor to minute boundary
    interval_ns = 1_000_000_000  # 1-second intervals

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = (args.host, args.port)

    print(f"Sending {args.ticks} ticks to {args.host}:{args.port} "
          f"(~{args.ticks // 60} complete 1-minute bars) ...")

    for i, tick in enumerate(generate_ticks(args.ticks, start_ns, interval_ns)):
        payload = json.dumps(tick).encode()
        sock.sendto(payload, dest)
        if (i + 1) % 60 == 0:
            print(f"  {i + 1} ticks sent — minute {(i + 1) // 60} complete")
        time.sleep(args.delay)

    # EOF sentinel
    sock.sendto(b'{"eof":true}', dest)
    print("Done — EOF sentinel sent.")
    sock.close()


if __name__ == "__main__":
    main()
