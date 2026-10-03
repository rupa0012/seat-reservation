# Seat Reservation at Scale

(README is completed at the end of the build. Quick start below.)

## Run locally

    docker compose up --build

API on http://localhost:8000 (docs at /docs). Admin token for `POST /shows`: `admin-secret`
(override with `ADMIN_TOKEN`). Users authenticate with `Authorization: Bearer <token>`;
the token is the user's identity.

## Burst test

    docker compose run --rm api python scripts/burst.py http://api:8000
