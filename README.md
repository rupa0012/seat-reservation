# Seat Reservation at Scale

A small JSON API that sells seats for shows without ever selling the same seat twice,
even when thousands of requests arrive at the same moment.

- Stack: Python 3.12, FastAPI, PostgreSQL 16, psycopg 3 (async, pooled), Prometheus metrics
- Design notes, trade-offs and honest AI-usage disclosure: see [WRITEUP.md](WRITEUP.md)

**Live URL:** `https://<YOUR-SERVICE>.onrender.com`  (fill in)
Free tier sleeps when idle: the first request after a quiet period can take up to ~1 minute
while the service wakes up. Check `/ready` and wait for `{"status":"ready"}`.

## Run locally (one command)

    docker compose up --build

API: http://localhost:8000 (interactive docs at `/docs`). Postgres runs in the same compose file.

Admin token locally: `admin-secret` (override with the `ADMIN_TOKEN` env var).

## Burst test (one command)

Creates a fresh show, hammers it, reconciles, prints PASS/FAIL (exit code 1 on failure).

    docker compose run --rm api python scripts/burst.py http://api:8000

Against the live service (needs `pip install httpx`):

    python scripts/burst.py https://<YOUR-SERVICE>.onrender.com --admin-token <ADMIN_TOKEN>

Useful flags: `--requests 15000 --concurrency 500 --seats 1000 --users 3000 --hot-seats 5 --hot-users 400`.
Lower `--requests` / `--concurrency` for small hosts.

Phases: A hot-seat storm, B on-sale stampede (~10% duplicate sends), C per-user limit under
parallelism, D idempotency, E identity (spoofed user, stranger cancel). Finally it checks
`available + held + confirmed == total_seats`, that no seat was sold twice, zero 5xx, and that the
`/metrics` counters match what the client saw.

## Authentication

Send `Authorization: Bearer <token>`.

- **Admin** (`POST /shows`): the token must equal the `ADMIN_TOKEN` env var.
- **Users**: demo scheme. The bearer token **is** the user id (`[A-Za-z0-9_.:@-]{1,64}`).
  Identity is taken only from the header. A `user_id` in a request body is ignored.
  A real deployment would verify a signed token (JWT) and use its `sub`; nothing else changes.

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| POST | `/shows` | admin | Create a show: `{"name","seats":["A1",...],"price_paise":25000,"per_user_limit":4}` (price is integer paise; limit defaults to 4) |
| GET | `/shows/{id}` | none | Counts + seat statuses + `reconciled` flag. `?seats=false` for counts only |
| POST | `/shows/{id}/reserve` | user | Reserve seats: `{"seats":["A1","A2"]}` + `Idempotency-Key` header (or `idempotency_key` field) |
| POST | `/reservations/{id}/cancel` | owner | Cancel; seats go back to `available` |
| GET | `/health` | none | Liveness (process is up; does not touch the DB) |
| GET | `/ready` | none | Readiness: 503 unless the DB answers and the schema exists (fails closed) |
| GET | `/metrics` | none | Prometheus text format |

### Reserve semantics

- **All-or-nothing**: a multi-seat request either gets every seat or none. If any seat is taken, nothing is held.
- Max 10 seats per request. Seats in one request must be unique.
- **Idempotency key is required** (422 otherwise). Same key + same seats returns the original result
  with header `Idempotent-Replay: true` (also for original declines). Same key + different seats returns 409 `idempotency_key_reuse`.
- Per-user limit counts seats the user already holds in that show plus the new request.

### Status codes and error codes

| Code | `error` | Meaning |
|---|---|---|
| 201 | n/a | Reservation confirmed |
| 401 | `unauthorized` | Missing / malformed bearer token |
| 403 | `forbidden` | Not admin, or cancelling someone else's reservation |
| 404 | `show_not_found`, `unknown_seat`, `reservation_not_found` | |
| 409 | `seat_taken` | A requested seat is not available |
| 409 | `per_user_limit` | Would exceed the per-user limit |
| 409 | `idempotency_key_reuse` | Key reused with a different request |
| 422 | `validation_error`, `idempotency_key_required` | Bad input (e.g. non-integer price) |
| 503 | `service_unavailable` | Database busy/unreachable; has `Retry-After: 1` |

Contention is a normal outcome and always a 409, never a 5xx.

## Observability

**Metrics** (`GET /metrics`):

- `reservations_confirmed_total`
- `reservations_declined_total{reason="seat-taken|per-user-limit|idempotent-replay|idempotency-key-conflict"}`
- `reservations_cancelled_total`
- `seats_available{show_id}` and `seats_by_status{show_id,status}`: read from Postgres at scrape time, so they always match the API
- `http_requests_total{method,route,status}`, `http_request_duration_seconds{route}`

**Logs**: one JSON line per request on stdout with `request_id`, method, path, status, `duration_ms`,
`user_id` and `outcome`. Send your own `X-Request-Id` to trace a request; it is echoed in the response header.
On Render: dashboard → the service → **Logs**. Locally: `docker compose logs -f api`.

## Configuration (env vars)

`DATABASE_URL`, `ADMIN_TOKEN`, `POOL_MIN` (5), `POOL_MAX` (20), `POOL_TIMEOUT` (30 s),
`STATEMENT_TIMEOUT_MS` (15000), `LOG_LEVEL` (INFO).

## Deploy

`render.yaml` is a Render Blueprint (web service + free Postgres). Set `ADMIN_TOKEN` when prompted.
Any host that runs the Dockerfile and provides `DATABASE_URL` works.

## Project layout

    app/main.py      HTTP layer: validation, auth, health, metrics, request-id logging
    app/service.py   All booking decisions (atomic, inside Postgres)
    app/schema.sql   Tables and constraints (applied automatically at startup, idempotent)
    app/db.py        Connection pool
    scripts/burst.py Load + reconciliation script
