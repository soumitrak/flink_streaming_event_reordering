#!/usr/bin/env python3
"""
Fake clickstream event producer for local testing.

Generates realistic e-commerce clickstream sessions and publishes them to a
Kafka topic in *out-of-order* fashion to exercise the Flink reordering logic.

Session structure
-----------------
Each (user_id, session_id) produces two phases of events:

  Phase 1 – The funnel (events within ≤55 seconds)
    • Either a Checkout-bound sequence  (e.g. HomePage → Search → ProductPage → AddToCart → Checkout)
    • Or an abandoned sequence          (e.g. HomePage → Search → CategoryPage)
    The Checkout event, if present, falls within 1 minute of the session's
    first event — exactly the window the Flink app scans for.

  Phase 2 – Post-session browsing (7–15 minutes after Phase 1)
    A few extra events for the *same* session ID that push maxEventTime beyond
    the 6-minute span needed to trigger Part-1 processing in the Flink app.

Out-of-order simulation
-----------------------
A configurable fraction of events receive a random arrival delay (0–5 min).
Events are then sorted by (event_time + delay) before being sent to Kafka,
so the broker receives them in a shuffled temporal order.
"""

import argparse
import json
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

# Funnel sequences that include a Checkout event (happy-path sessions)
CHECKOUT_FUNNELS = [
    ["HomePage", "Search",        "ProductPage", "AddToCart", "Checkout"],
    ["HomePage", "CategoryPage",  "ProductPage", "AddToCart", "Checkout"],
    ["Search",   "ProductPage",   "AddToCart",   "Checkout"],
    ["HomePage", "ProductPage",   "WishList",    "AddToCart", "Checkout"],
    ["CategoryPage", "ProductPage", "Checkout"],
    ["HomePage", "ProductPage",   "AddToCart",   "Checkout"],
]

# Funnel sequences that do NOT include a Checkout (abandoned browsing)
ABANDONED_FUNNELS = [
    ["HomePage", "Search",       "CategoryPage", "ProductPage"],
    ["HomePage", "WishList",     "Search"],
    ["Search",   "CategoryPage", "ProductPage",  "WishList"],
    ["HomePage", "CategoryPage"],
    ["Search",   "ProductPage"],
]

# Event names used in the post-session browsing phase
POST_SESSION_EVENTS = ["HomePage", "Search", "CategoryPage", "ProductPage", "WishList"]

# Events that carry a product ID / price in their properties
PRODUCT_EVENTS = {"ProductPage", "AddToCart", "Checkout"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt_time(dt: datetime) -> str:
    """Format datetime as the app's event_time string (microsecond precision)."""
    return dt.strftime(EVENT_TIME_FMT)


def make_event(user_id: int, session_id: str, event_name: str, event_time: datetime) -> dict:
    """Build a single clickstream JSON event with realistic nested fields."""
    return {
        # --- Required fields (parsed by the Flink app) ---
        "user_id":    user_id,
        "session_id": session_id,
        "event_time": fmt_time(event_time),
        "event_name": event_name,
        # --- Extra nested fields (ignored by the Flink app) ---
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
        # Internal key used only for arrival-order simulation; not part of the schema.
        "_ts_ms": int(event_time.timestamp() * 1000),
    }


def generate_session(user_id: int, base_time: datetime, checkout: bool = True) -> list:
    """
    Generate all events for one (user_id, session_id) pair.

    Phase 1 – tight funnel within ~55 seconds.
    Phase 2 – post-session browsing 7–15 minutes later (triggers the 6-min span check).
    """
    session_id = str(uuid4())
    events = []

    # ---------- Phase 1: the funnel ----------
    funnel = random.choice(CHECKOUT_FUNNELS if checkout else ABANDONED_FUNNELS)
    # Space events evenly within [0, 55] seconds so Checkout ≤ 55 s from start.
    interval = 55.0 / max(len(funnel) - 1, 1)
    t = base_time
    for idx, name in enumerate(funnel):
        events.append(make_event(user_id, session_id, name, t))
        t += timedelta(seconds=interval + random.uniform(-2, 2))

    # ---------- Phase 2: post-session browsing ----------
    # These events share the same session_id but appear 7–15 minutes later.
    # Their purpose is to push maxEventTime far enough from minEventTime so
    # the Flink "span > 6 min" condition fires and Part-1 processing runs.
    post_count = random.randint(2, 4)
    t = base_time + timedelta(minutes=random.uniform(7, 15))
    chosen = random.sample(POST_SESSION_EVENTS, k=min(post_count, len(POST_SESSION_EVENTS)))
    for name in chosen:
        events.append(make_event(user_id, session_id, name, t))
        t += timedelta(minutes=random.uniform(0.5, 2.5))

    return events


def apply_out_of_order(events: list, late_ratio: float, max_late_secs: float) -> list:
    """
    Shuffle events to simulate out-of-order arrival.

    Each event is assigned an "arrival time" = event_time + random_delay.
    The delay is 0 for (1 - late_ratio) events and U[0, max_late_secs] seconds
    for the rest.  Events are then sorted by arrival time before sending.
    """
    def arrival_key(ev: dict) -> float:
        delay_ms = (
            random.uniform(0, max_late_secs * 1_000)
            if random.random() < late_ratio
            else 0.0
        )
        return ev["_ts_ms"] + delay_ms

    return sorted(events, key=arrival_key)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Produce fake out-of-order clickstream events to Kafka."
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
        "-n", "--num-events", type=int, default=100,
        help="Approximate number of events to produce (default: 100)",
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
        help="Fraction of events sent out-of-order (default: 0.3)",
    )
    ap.add_argument(
        "--max-late-secs", type=float, default=300.0,
        help="Maximum arrival delay in seconds for late events (default: 300)",
    )
    ap.add_argument(
        "--send-interval", type=float, default=0,
        help="Pause between consecutive Kafka sends in seconds (default: 0, no pause)",
    )
    args = ap.parse_args()

    # ---- connect -------------------------------------------------------
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

    # ---- summary -------------------------------------------------------
    user_ids = list(range(1001, 1001 + args.num_users))
    print(
        f"\n{'─'*55}\n"
        f"  Topic            : {args.topic}\n"
        f"  Target events    : {args.num_events}\n"
        f"  Simulated users  : {args.num_users}  {user_ids}\n"
        f"  Checkout ratio   : {args.checkout_ratio:.0%}\n"
        f"  Late-arrival     : {args.late_ratio:.0%} of events "
        f"(up to {args.max_late_secs:.0f} s late)\n"
        f"{'─'*55}\n"
    )

    # ---- generate sessions ---------------------------------------------
    # Session start times are spread over the past hour so the Flink buffer
    # accumulates events spanning well beyond the 6-minute trigger threshold.
    base_time = datetime.utcnow() - timedelta(hours=1)
    all_events: list = []
    session_count = 0

    while len(all_events) < args.num_events:
        user_id = random.choice(user_ids)
        session_start = base_time + timedelta(
            minutes=random.uniform(0, 50),
            seconds=random.uniform(0, 59),
        )
        is_checkout = random.random() < args.checkout_ratio
        events = generate_session(user_id, session_start, checkout=is_checkout)
        all_events.extend(events)
        session_count += 1

    # Trim to the requested count (excess comes from the last session batch).
    all_events = all_events[: args.num_events]

    # ---- simulate out-of-order delivery --------------------------------
    all_events = apply_out_of_order(all_events, args.late_ratio, args.max_late_secs)

    # ---- produce -------------------------------------------------------
    print(
        f"Producing {len(all_events)} events from {session_count} sessions …\n",
        flush=True,
    )

    sent = 0
    errors = 0
    for event in all_events:
        # Remove the internal arrival-simulation key before sending.
        clean_event = {k: v for k, v in event.items() if k != "_ts_ms"}

        key = f"{event['user_id']}_{event['session_id']}"
        try:
            producer.send(args.topic, key=key, value=clean_event)
            sent += 1
        except Exception as exc:
            print(f"  [WARN] Send failed: {exc}", file=sys.stderr)
            errors += 1

        # Progress line every 1000 events
        if sent % 1000 == 0 or sent == len(all_events):
            print(
                f"  [{sent:>4}/{len(all_events)}]"
                f"  user={event['user_id']}"
                f"  session={event['session_id'][:8]}…"
                f"  event={event['event_name']:<15s}"
                f"  time={event['event_time']}",
                flush=True,
            )

        if args.send_interval > 0:
            time.sleep(args.send_interval)

    producer.flush()
    producer.close()

    print(
        f"\n{'─'*55}\n"
        f"  Sent   : {sent}\n"
        f"  Errors : {errors}\n"
        f"  Topic  : {args.topic}\n"
        f"{'─'*55}"
    )
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
