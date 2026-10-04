# WRITEUP

Live URL: `https://seat-reservation-ig71.onrender.com` · Repo: https://github.com/rupa0012/seat-reservation

## 1. How a seat is never sold twice (the atomic mechanism)

The decision is made **inside Postgres**, in one statement per seat, never as "read, then write" in Python:

    UPDATE seats SET status='confirmed', user_id=:u, reservation_id=:r
    WHERE show_id=:s AND label=:l AND status='available'

When many transactions try the same row, Postgres lets one change it and makes the others wait for the
row lock. Each waiter then re-checks the `WHERE` clause, sees `status` is no longer `available`, and
matches 0 rows. My code turns "0 rows" into a clean `409 seat_taken`. So the database, not application
memory, is the single point of truth, and it keeps working with more than one app instance.

Multi-seat requests are **all-or-nothing**. The seats are claimed inside a savepoint (a nested
transaction). If any seat comes back 0 rows, only that partial claim is rolled back, the decline is
recorded, and the user gets 409 with the seat that failed. A person never ends up with 2 of the 3 seats they asked for.

**Deadlocks:** two requests wanting seats {A1,A2} and {A2,A1} could each lock one and wait on the other.
Every request sorts and de-duplicates its seats and locks them in that same order, so a wait cycle cannot form.
A retry (max 4) on deadlock/serialization errors exists as a safety net; it should not fire.

**Schema guard:** a `CHECK` constraint makes "available but owned" (and "owned but not available") unrepresentable,
so even a bug in my code cannot leave a half-owned seat.

**Per-user limit under parallelism:** the count-then-insert would race if one user fires 10 parallel
requests. Each reserve first takes `pg_advisory_xact_lock(hash(user_id))`, which serialises only that one
user's requests (other users are unaffected) and is released automatically at commit. The check and the
claim therefore see a consistent count. Default limit is 4 per show (`per_user_limit`).

**Money** is integer paise everywhere (`price_paise * seats`). Floats are rejected at the API (`strict` int).

## 2. Idempotency

Clients must send an `Idempotency-Key` (header or body field). In the **same transaction** as the booking
I store the outcome in `idempotency_keys` keyed by `(user_id, key)`, with a hash of `show_id + sorted seats`.

- Same key + same request: return the stored status and body again, with `Idempotent-Replay: true`. This also applies to stored **declines**, so a retried failure fails the same way.
- Same key + different seats: `409 idempotency_key_reuse` (a key must mean one request).
- Because the booking and the key commit together, a crash can never leave "seats taken but key not remembered", which would cause a double charge on retry.
- Two retries arriving at the same time are serialised by the per-user advisory lock, so the second one finds the first one's stored result.
- Replay for a successful booking returns the reservation's current state (it may have been cancelled since).
- Keys are scoped per user, so one user cannot replay or collide with another user's key.

Metrics label these as `idempotent-replay` (not counted again as a confirmed sale).

## 3. Holds, cancel and expiry (what I built and what I did not)

**Built:** a reservation is confirmed immediately (`confirmed`). `POST /reservations/{id}/cancel` is owner-only,
locks the reservation row (`FOR UPDATE`, so a double-cancel is safe), and frees only seats still carrying that
reservation's id, so a release can never touch a seat that has since gone to someone else. Cancelling twice is a no-op.

**Not built (honest gap):** time-based **expiry of holds**. The seat status `held` exists in the schema and in the
invariant (`available + held + confirmed == total_seats`), but no code path creates a hold today, so `held` is always 0.
How I would add it: a `hold` endpoint writing `status='held'` with `expires_at`; a reaper (a periodic
`UPDATE seats SET status='available' ... WHERE status='held' AND expires_at < now()`) plus treating expired holds as
available inside the same atomic `UPDATE`, so correctness never depends on the reaper being on time; and confirming a hold
only if it is still unexpired and owned by the caller.

## 4. Consistency vs availability

I chose **consistency**: never double-sell, even if that means some requests wait or are refused.
All decisions go through one Postgres primary and a bounded connection pool (20; 10 on the free host). Thousands of concurrent requests
queue in the app and only a few talk to the database at once. This is deliberate backpressure.

- Contention (seat gone, limit hit) is an expected business result: **409, never 5xx**.
- If the database is down or saturated, I fail **closed**: `/ready` returns 503, and requests get `503` with `Retry-After`
  rather than guessing a seat is free. A pool wait is capped (30 s) and each statement has a 15 s timeout, so requests fail fast rather than hang forever.
- `/health` (liveness) does not touch the DB, so a DB outage does not make the platform restart a healthy process; `/ready` does touch it.
- Cost of this choice: throughput is bounded by one Postgres. Hot seats serialise on a row lock (that is the point). Scaling beyond that needs sharding by show, not a looser guarantee.
- The seat counts in `/shows/{id}` come from one SQL statement (a consistent snapshot), so `reconciled` is exact, not approximate.

## 5. What I would alert on at 2am

Page me (user impact, now):
1. **Invariant broken**: `available + held + confirmed != total_seats` for any show, or any seat sold twice. This should never fire; if it does, stop sales on that show.
2. **5xx rate** on `/shows/*/reserve` above ~1% for 5 min (`http_requests_total{status=~"5.."}`).
3. **`/ready` failing** (database unreachable) for more than 1–2 min.
4. **p99 reserve latency** above ~2 s for 5 min (`http_request_duration_seconds`), the early sign of pool/DB saturation.

Ticket in the morning (not a page):
- `reservations_declined_total{reason="seat-taken"}` ratio rising is normal during an on-sale; a drop of `reservations_confirmed_total` to 0 during a sale is not.
- Pool-wait timeouts / `503 service_unavailable` appearing in logs.
- Spike of `idempotency-key-conflict` (a client bug).

Debugging path: take the `X-Request-Id` from the failing response, grep it in the JSON logs, and read `outcome`, `status`, `duration_ms`.

## 6. How I used AI (honest account)

I used an AI assistant (Claude) heavily, as the assignment allowed. To be clear about who did what:

**What I directed / decided myself:** 
- Picked the scope and the stack with the assistant's options in front of me (Python/FastAPI + Postgres on Docker, Render for hosting).
- Decided how to deploy, installed Docker and fixed my Windows virtualization/WSL problem myself.
- Ran the burst tests on my machine, read the output and reported the failures back.
- Decided on commit structure and what to submit.

**What the AI decided / wrote:**
- Most of the code (`app/`, `scripts/burst.py`, Dockerfile, compose, Render blueprint) and the first drafts of this README and write-up.
- The core design (conditional `UPDATE`, sorted lock order, advisory lock per user, idempotency table stored in the same transaction).

**Where testing found real problems (not AI-guessable up front):**
- The first full-size burst stalled: my load client, not the server, was the bottleneck (thousands of tasks queued inside the HTTP client). Fixed by bounding in-flight requests.
- A one-off connection reset during a run led to raising the server keep-alive timeout to 75 s.
- The AI could not run the Python code in its own sandbox (no package access), so the SQL logic was tested directly with `psql`/`pgbench`, and the first end-to-end Python run was on my machine.
- I have **not** yet seen the full 20,000-request burst finish on my laptop; the medium run passed all checks. *

## 7. Next steps (not done)

1. **Real hold expiry** (see section 3) plus a hold endpoint.
2. **Real authentication**: signed JWTs; today the bearer token is the user id (demo scheme).
3. **Rate limiting** per user/IP to protect the pool from one abusive client.
4. **Read replicas / cache** for `GET /shows/{id}`, so reads never compete with writes on a hot show.
5. **Sharding by show** and a bigger Postgres if a single show's write rate exceeds one primary.
6. **Idempotency key cleanup** (TTL job) and a unique constraint on seat labels per show validated at creation for very large halls.
7. **Tracing** (OpenTelemetry) and Grafana dashboards for the metrics above, with the alerts in section 5 defined as code.
8. Pin dependency versions (`requirements.txt` currently uses lower bounds) and add CI running the burst against a compose stack.
