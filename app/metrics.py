"""Prometheus metrics.

Counters live in process memory (this service runs a single worker; see WRITEUP).
Seat gauges are NOT kept in memory: they are read from Postgres at scrape time
(see main.py /metrics), so they can never drift from the real state.
"""
from prometheus_client import CollectorRegistry, Counter, Histogram

registry = CollectorRegistry()

RESERVATIONS_CONFIRMED = Counter(
    "reservations_confirmed_total", "Reservations confirmed (fresh, not replays).", registry=registry
)
RESERVATIONS_DECLINED = Counter(
    "reservations_declined_total",
    "Reservation requests that did not create a new reservation, by reason.",
    ["reason"],
    registry=registry,
)
RESERVATIONS_CANCELLED = Counter(
    "reservations_cancelled_total", "Reservations cancelled by their owner.", registry=registry
)
HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests.", ["method", "route", "status"], registry=registry
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency.",
    ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
    registry=registry,
)

# Declined reasons (label values match the assignment wording).
SEAT_TAKEN = "seat-taken"
PER_USER_LIMIT = "per-user-limit"
IDEMPOTENT_REPLAY = "idempotent-replay"
IDEMPOTENCY_CONFLICT = "idempotency-key-conflict"

# Make every reason show up with value 0 from the start, so dashboards/tests can rely on it.
for _reason in (SEAT_TAKEN, PER_USER_LIMIT, IDEMPOTENT_REPLAY, IDEMPOTENCY_CONFLICT):
    RESERVATIONS_DECLINED.labels(reason=_reason)
