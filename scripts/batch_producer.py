#!/usr/bin/env python3
"""
Batch clickstream event producer using tumbling windows.

Divides the time range [now - start_before_hours, now] into consecutive tumbling
windows of ``window_size`` seconds.  For each window ``batch_size`` events are
generated whose session-start times fall within that window, then the batch is
published to Kafka before the script advances to the next window.

This lets you replay a historical period at a controlled rate while preserving
the relative temporal structure that the Flink reordering pipeline expects.

Session structure
-----------------
Identical to producer.py:

  Phase 1 – The funnel  (events within ≤55 seconds of the session start)
  Phase 2 – Post-session browsing  (7–15 minutes later, same session_id)

Out-of-order simulation
-----------------------
Applied independently per window so each batch exercises the Flink watermark
logic on its own before the next batch arrives.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timedelta
from uuid import uuid4

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
fake = Faker()

EVENT_TIME_FMT = "%d/%m/%Y %H:%M:%S.%f"

CHECKOUT_FUNNELS = [
    ["HomePage", "Search",        "ProductPage", "AddToCart", "Checkout"],
    ["HomePage", "CategoryPage",  "ProductPage", "AddToCart", "Checkout"],
    ["Search",   "ProductPage",   "AddToCart",   "Checkout"],
    ["HomePage", "ProductPage",   "WishList",    "AddToCart", "Checkout"],
    ["CategoryPage", "ProductPage", "Checkout"],
    ["HomePage", "ProductPage",   "AddToCart",   "Checkout"],
]

ABANDONED_FUNNELS = [
    ["HomePage", "Search",       "CategoryPage", "ProductPage"],
    ["HomePage", "WishList",     "Search"],
    ["Search",   "CategoryPage", "ProductPage",  "WishList"],
    ["HomePage", "CategoryPage"],
    ["Search",   "ProductPage"],
]

POST_SESSION_EVENTS = ["HomePage", "Search", "CategoryPage", "ProductPage", "WishList"]
PRODUCT_EVENTS      = {"ProductPage", "AddToCart", "Checkout"}


# ---------------------------------------------------------------------------
# Helpers (shared with producer.py)
# ---------------------------------------------------------------------------

def fmt_time(dt: datetime) -> str:
    return dt.strftime(EVENT_TIME_FMT)


def make_event(user_id: int, session_id: str, event_name: str, event_time: datetime) -> dict:
    return {
        "user_id":    user_id,
        "session_id": session_id,
        "event_time": fmt_time(event_time),
        "event_name": event_name,
        "properties": {
            "page_url":   f"https://shop.example.com/{event_name.lower()}",
            "referrer":   fake.url(),
            "product_id": str(uuid4()) if event_name in PRODUCT_EVENTS else None,
            "price":      round(random.uniform(9.99, 499.99), 2)
                          if event_name in {"AddToCart", "Checkout"} else None,
        },
        "context": {
            "ip":         fake.ipv4_private(),
            "user_agent": fake.user_agent(),
            "country":    fake.country_code(),
            "device":     random.choice(["desktop", "mobile", "tablet"]),
        },
        "_ts_ms": int(event_time.timestamp() * 1000),
    }


def generate_session(user_id: int, base_time: datetime, checkout: bool = True) -> list:
    session_id = str(uuid4())
    events = []

    funnel   = random.choice(CHECKOUT_FUNNELS if checkout else ABANDONED_FUNNELS)
    interval = 55.0 / max(len(funnel) - 1, 1)
    t = base_time
    for name in funnel:
        events.append(make_event(user_id, session_id, name, t))
        t += timedelta(seconds=interval + random.uniform(-2, 2))

    post_count = random.randint(2, 4)
    t = base_time + timedelta(minutes=random.uniform(7, 15))
    chosen = random.sample(POST_SESSION_EVENTS, k=min(post_count, len(POST_SESSION_EVENTS)))
    for name in chosen:
        events.append(make_event(user_id, session_id, name, t))
        t += timedelta(minutes=random.uniform(0.5, 2.5))

    return events


def apply_out_of_order(events: list, late_ratio: float, max_late_secs: float) -> list:
    def arrival_key(ev: dict) -> float:
        delay_ms = (
            random.uniform(0, max_late_secs * 1_000)
            if random.random() < late_ratio
            else 0.0
        )
        return ev["_ts_ms"] + delay_ms

    return sorted(events, key=arrival_key)


# ---------------------------------------------------------------------------
# Tumbling-window batch generation
# ---------------------------------------------------------------------------

def build_windows(start: datetime, end: datetime, window_size_secs: int) -> list[tuple[datetime, datetime]]:
    """Return a list of (window_start, window_end) tuples covering [start, end)."""
    windows = []
    ws = timedelta(seconds=window_size_secs)
    t = start
    while t < end:
        windows.append((t, min(t + ws, end)))
        t += ws
    return windows


def generate_batch(
    user_ids: list[int],
    window_start: datetime,
    window_end: datetime,
    batch_size: int,
    checkout_ratio: float,
) -> tuple[list, int]:
    """
    Generate at least ``batch_size`` events whose session-start times fall
    within [window_start, window_end).

    Returns (events, session_count).
    """
    span_secs = max((window_end - window_start).total_seconds() - 1, 0)
    events: list = []
    session_count = 0

    while len(events) < batch_size:
        user_id      = random.choice(user_ids)
        session_start = window_start + timedelta(seconds=random.uniform(0, span_secs))
        is_checkout  = random.random() < checkout_ratio
        events.extend(generate_session(user_id, session_start, checkout=is_checkout))
        session_count += 1

    # Trim to exactly batch_size (excess comes from the last session).
    return events[:batch_size], session_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Produce fake clickstream events in tumbling-window batches to Kafka."
    )
    ap.add_argument(
        "--bootstrap-servers", default="localhost:9094",
        help="Kafka bootstrap servers (default: localhost:9094)",
    )
    ap.add_argument(
        "--topic", default="clickstream",
        help="Destination Kafka topic (default: clickstream)",
    )
    ap.add_argument(
        "--start-before", type=float, default=1.0,
        help="How many hours in the past the first window starts (default: 1)",
    )
    ap.add_argument(
        "--window-size", type=int, default=60,
        help="Tumbling window duration in seconds (default: 60)",
    )
    ap.add_argument(
        "--batch-size", type=int, default=1000,
        help="Number of events to produce per window (default: 1000)",
    )
    ap.add_argument(
        "--num-users", type=int, default=5,
        help="Number of distinct simulated user IDs (default: 5)",
    )
    ap.add_argument(
        "--checkout-ratio", type=float, default=0.7,
        help="Fraction of sessions that include a Checkout event (default: 0.7)",
    )
    ap.add_argument(
        "--late-ratio", type=float, default=0.3,
        help="Fraction of events sent out-of-order within each batch (default: 0.3)",
    )
    ap.add_argument(
        "--max-late-secs", type=float, default=60.0,
        help="Maximum arrival delay in seconds for late events (default: 60)",
    )
    ap.add_argument(
        "--send-interval", type=float, default=0.0,
        help="Pause between consecutive Kafka sends in seconds (default: 0)",
    )
    ap.add_argument(
        "--inter-batch-delay", type=float, default=0.0,
        help="Pause between finishing one batch and starting the next, in seconds (default: 0)",
    )
    args = ap.parse_args()

    # ---- connect -----------------------------------------------------------
    print(f"Connecting to Kafka at {args.bootstrap_servers} …", flush=True)
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=500,
        )
    except NoBrokersAvailable:
        print(
            f"\nERROR: No Kafka brokers reachable at '{args.bootstrap_servers}'.\n"
            "Make sure the stack is running:  make up",
            file=sys.stderr,
        )
        sys.exit(1)

    # ---- derive windows ----------------------------------------------------
    now        = datetime.utcnow()
    start_time = now - timedelta(hours=args.start_before)
    windows    = build_windows(start_time, now, args.window_size)
    user_ids   = list(range(1001, 1001 + args.num_users))

    total_events = len(windows) * args.batch_size

    print(
        f"\n{'─'*60}\n"
        f"  Topic              : {args.topic}\n"
        f"  Time range         : {start_time.strftime('%H:%M:%S')} → {now.strftime('%H:%M:%S')} UTC\n"
        f"  Window size        : {args.window_size} s\n"
        f"  Number of windows  : {len(windows)}\n"
        f"  Batch size         : {args.batch_size} events/window\n"
        f"  Total target events: {total_events}\n"
        f"  Simulated users    : {args.num_users}  {user_ids}\n"
        f"  Checkout ratio     : {args.checkout_ratio:.0%}\n"
        f"  Late-arrival       : {args.late_ratio:.0%} of events "
        f"(up to {args.max_late_secs:.0f} s late)\n"
        f"{'─'*60}\n",
        flush=True,
    )

    # ---- produce window by window ------------------------------------------
    total_sent   = 0
    total_errors = 0

    for win_idx, (win_start, win_end) in enumerate(windows, start=1):
        batch, session_count = generate_batch(
            user_ids, win_start, win_end, args.batch_size, args.checkout_ratio
        )
        batch = apply_out_of_order(batch, args.late_ratio, args.max_late_secs)

        print(
            f"[Window {win_idx:>3}/{len(windows)}]  "
            f"{win_start.strftime('%H:%M:%S')} – {win_end.strftime('%H:%M:%S')}  "
            f"({len(batch)} events, {session_count} sessions)",
            flush=True,
        )

        sent   = 0
        errors = 0
        for event in batch:
            clean_event = {k: v for k, v in event.items() if k != "_ts_ms"}
            key = f"{event['user_id']}_{event['session_id']}"
            try:
                producer.send(args.topic, key=key, value=clean_event)
                sent += 1
            except Exception as exc:
                print(f"  [WARN] Send failed: {exc}", file=sys.stderr)
                errors += 1

            if args.send_interval > 0:
                time.sleep(args.send_interval)

        producer.flush()
        total_sent   += sent
        total_errors += errors

        print(
            f"           sent={sent}  errors={errors}  "
            f"cumulative={total_sent}/{total_events}",
            flush=True,
        )

        if args.inter_batch_delay > 0 and win_idx < len(windows):
            time.sleep(args.inter_batch_delay)

    # ---- summary -----------------------------------------------------------
    producer.close()
    print(
        f"\n{'─'*60}\n"
        f"  Windows  : {len(windows)}\n"
        f"  Sent     : {total_sent}\n"
        f"  Errors   : {total_errors}\n"
        f"  Topic    : {args.topic}\n"
        f"{'─'*60}"
    )
    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()