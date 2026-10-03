"""Business logic. Every correctness decision is made INSIDE Postgres, atomically.

The one rule that prevents double-selling:

    UPDATE seats SET status='confirmed', ... WHERE ... AND status='available'

Postgres lets exactly one concurrent transaction change a given row from 'available'.
Every other transaction waits for the row lock, re-checks the WHERE clause, matches
0 rows, and we turn that into a clean 409. We never "read, then write".
"""
import asyncio
import hashlib
import logging
import uuid
from dataclasses import dataclass, field

from psycopg import errors
from psycopg.types.json import Jsonb

from . import db, metrics

log = logging.getLogger("app.service")

MAX_RETRIES = 4


# ----------------------------------------------------------------------------- errors / results
class ShowNotFound(Exception):
    pass


class UnknownSeat(Exception):
    def __init__(self, label: str):
        self.label = label


class ReservationNotFound(Exception):
    pass


class NotOwner(Exception):
    pass


class _SeatUnavailable(Exception):
    """Raised inside the savepoint to roll back a partial claim (all-or-nothing)."""

    def __init__(self, label: str, exists: bool):
        self.label = label
        self.exists = exists


@dataclass
class Outcome:
    status_code: int
    body: dict
    kind: str  # "confirmed" or one of the metrics decline reasons
    replay: bool = False
    headers: dict = field(default_factory=dict)


def _reservation_body(row: dict) -> dict:
    return {
        "reservation_id": str(row["id"]),
        "show_id": str(row["show_id"]),
        "user_id": row["user_id"],
        "seats": list(row["seats"]),
        "amount_paise": row["amount_paise"],
        "status": row["status"],
    }


# ----------------------------------------------------------------------------- shows
async def create_show(name: str, seats: list[str], price_paise: int, per_user_limit: int) -> dict:
    show_id = uuid.uuid4()
    async with db.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO shows (id, name, price_paise, per_user_limit, total_seats) "
                "VALUES (%s, %s, %s, %s, %s)",
                (show_id, name, price_paise, per_user_limit, len(seats)),
            )
            await conn.execute(
                "INSERT INTO seats (show_id, label, pos) "
                "SELECT %s, t.label, t.pos FROM unnest(%s::text[]) WITH ORDINALITY AS t(label, pos)",
                (show_id, seats),
            )
    return await show_state(show_id, include_seats=True)


async def show_state(show_id: uuid.UUID, include_seats: bool = True) -> dict:
    """One SQL statement = one consistent snapshot, so the invariant can be checked exactly."""
    async with db.pool.connection() as conn:
        if include_seats:
            cur = await conn.execute(
                "SELECT sh.name, sh.price_paise, sh.per_user_limit, sh.total_seats, se.label, se.status "
                "FROM shows sh LEFT JOIN seats se ON se.show_id = sh.id "
                "WHERE sh.id = %s ORDER BY se.pos",
                (show_id,),
            )
            rows = await cur.fetchall()
            if not rows:
                raise ShowNotFound()
            counts = {"available": 0, "held": 0, "confirmed": 0}
            seats = []
            for r in rows:
                if r["label"] is None:
                    continue
                counts[r["status"]] += 1
                seats.append({"label": r["label"], "status": r["status"]})
            head = rows[0]
        else:
            cur = await conn.execute(
                "SELECT sh.name, sh.price_paise, sh.per_user_limit, sh.total_seats, "
                "  count(se.label) FILTER (WHERE se.status = 'available') AS available, "
                "  count(se.label) FILTER (WHERE se.status = 'held')      AS held, "
                "  count(se.label) FILTER (WHERE se.status = 'confirmed') AS confirmed "
                "FROM shows sh LEFT JOIN seats se ON se.show_id = sh.id "
                "WHERE sh.id = %s GROUP BY sh.id",
                (show_id,),
            )
            head = await cur.fetchone()
            if head is None:
                raise ShowNotFound()
            counts = {k: head[k] for k in ("available", "held", "confirmed")}
            seats = None

    body = {
        "id": str(show_id),
        "name": head["name"],
        "price_paise": head["price_paise"],
        "per_user_limit": head["per_user_limit"],
        "total_seats": head["total_seats"],
        "counts": counts,
        "reconciled": sum(counts.values()) == head["total_seats"],
    }
    if seats is not None:
        body["seats"] = seats
    return body


# ----------------------------------------------------------------------------- reserve
async def reserve(show_id: uuid.UUID, user_id: str, key: str, seats: list[str]) -> Outcome:
    """All-or-nothing: either every requested seat is confirmed for the user, or none is."""
    for attempt in range(MAX_RETRIES):
        try:
            return await _reserve_once(show_id, user_id, key, seats)
        except (errors.DeadlockDetected, errors.SerializationFailure):
            # Should not happen (seats are locked in sorted order) but retrying is always safe:
            # the whole transaction was rolled back.
            if attempt == MAX_RETRIES - 1:
                raise
            log.warning("transaction retry", extra={"fields": {"attempt": attempt + 1}})
            await asyncio.sleep(0.01 * (attempt + 1))
    raise RuntimeError("unreachable")


async def _reserve_once(show_id: uuid.UUID, user_id: str, key: str, seats: list[str]) -> Outcome:
    # Sorted + de-duplicated: every request takes seat locks in the same global order,
    # so two multi-seat requests can never wait on each other in a cycle (no deadlock).
    wanted = sorted(set(seats))
    request_hash = hashlib.sha256(f"{show_id}|{','.join(wanted)}".encode()).hexdigest()

    async with db.pool.connection() as conn:
        async with conn.transaction():  # BEGIN ... COMMIT (ROLLBACK if anything raises)
            # 1) Serialize this user's requests. This makes the idempotency check and the
            #    per-user limit race-free, even when one user fires parallel requests.
            #    Released automatically at COMMIT/ROLLBACK. Other users are not affected.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s::text, 0))", (user_id,))

            # 2) Idempotency: has this (user, key) been seen before?
            cur = await conn.execute(
                "SELECT request_hash, status_code, response, reservation_id "
                "FROM idempotency_keys WHERE user_id = %s AND key = %s",
                (user_id, key),
            )
            prev = await cur.fetchone()
            if prev is not None:
                if prev["request_hash"] != request_hash:
                    return Outcome(
                        409,
                        {
                            "error": "idempotency_key_reuse",
                            "message": "this idempotency key was already used with a different request",
                        },
                        metrics.IDEMPOTENCY_CONFLICT,
                    )
                body = prev["response"]
                if prev["reservation_id"] is not None:
                    # Return the reservation as it is now (it may have been cancelled since).
                    cur = await conn.execute(
                        "SELECT id, show_id, user_id, seats, amount_paise, status "
                        "FROM reservations WHERE id = %s",
                        (prev["reservation_id"],),
                    )
                    row = await cur.fetchone()
                    if row is not None:
                        body = _reservation_body(row)
                return Outcome(
                    prev["status_code"], body, metrics.IDEMPOTENT_REPLAY, replay=True,
                    headers={"Idempotent-Replay": "true"},
                )

            # 3) Show + per-user limit.
            cur = await conn.execute(
                "SELECT price_paise, per_user_limit FROM shows WHERE id = %s", (show_id,)
            )
            show = await cur.fetchone()
            if show is None:
                raise ShowNotFound()

            cur = await conn.execute(
                "SELECT count(*) AS n FROM seats "
                "WHERE show_id = %s AND user_id = %s AND status IN ('held', 'confirmed')",
                (show_id, user_id),
            )
            held = (await cur.fetchone())["n"]
            if held + len(wanted) > show["per_user_limit"]:
                body = {
                    "error": "per_user_limit",
                    "message": f"limit is {show['per_user_limit']} seats per user for this show",
                    "limit": show["per_user_limit"],
                    "already_held": held,
                    "requested": len(wanted),
                }
                await _store_decline(conn, user_id, key, request_hash, body)
                return Outcome(409, body, metrics.PER_USER_LIMIT)

            # 4) Claim the seats. Savepoint = nested `transaction()`: if any seat is
            #    unavailable we roll back only the partial claim, then record the decline.
            reservation_id = uuid.uuid4()
            amount = show["price_paise"] * len(wanted)  # integer paise, never floats
            try:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status) "
                        "VALUES (%s, %s, %s, %s::text[], %s, 'confirmed')",
                        (reservation_id, show_id, user_id, wanted, amount),
                    )
                    for label in wanted:  # sorted order == deadlock-free lock order
                        cur = await conn.execute(
                            "UPDATE seats SET status = 'confirmed', user_id = %s, reservation_id = %s "
                            "WHERE show_id = %s AND label = %s AND status = 'available'",
                            (user_id, reservation_id, show_id, label),
                        )
                        if cur.rowcount == 0:
                            cur = await conn.execute(
                                "SELECT 1 FROM seats WHERE show_id = %s AND label = %s", (show_id, label)
                            )
                            raise _SeatUnavailable(label, exists=await cur.fetchone() is not None)
            except _SeatUnavailable as e:
                if not e.exists:
                    raise UnknownSeat(e.label)  # outer transaction rolls back; nothing is stored
                body = {
                    "error": "seat_taken",
                    "message": f"seat {e.label} is not available",
                    "seat": e.label,
                }
                await _store_decline(conn, user_id, key, request_hash, body)
                return Outcome(409, body, metrics.SEAT_TAKEN)

            # 5) Success: save the result under the idempotency key in the SAME transaction,
            #    so "seats taken" and "key remembered" commit together or not at all.
            cur = await conn.execute(
                "SELECT id, show_id, user_id, seats, amount_paise, status FROM reservations WHERE id = %s",
                (reservation_id,),
            )
            body = _reservation_body(await cur.fetchone())
            await conn.execute(
                "INSERT INTO idempotency_keys (user_id, key, request_hash, status_code, response, reservation_id) "
                "VALUES (%s, %s, %s, 201, %s, %s)",
                (user_id, key, request_hash, Jsonb(body), reservation_id),
            )
            return Outcome(201, body, "confirmed")


async def _store_decline(conn, user_id: str, key: str, request_hash: str, body: dict) -> None:
    await conn.execute(
        "INSERT INTO idempotency_keys (user_id, key, request_hash, status_code, response) "
        "VALUES (%s, %s, %s, 409, %s)",
        (user_id, key, request_hash, Jsonb(body)),
    )


# ----------------------------------------------------------------------------- cancel
async def cancel(reservation_id: uuid.UUID, user_id: str) -> tuple[dict, bool]:
    """Owner-only. Returns (reservation, released_now)."""
    async with db.pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                "SELECT id, show_id, user_id, seats, amount_paise, status "
                "FROM reservations WHERE id = %s FOR UPDATE",  # serializes double-cancels
                (reservation_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise ReservationNotFound()
            if row["user_id"] != user_id:
                raise NotOwner()
            if row["status"] == "cancelled":
                return _reservation_body(row), False

            # Only free seats that still belong to THIS reservation. The reservation_id guard
            # means a release can never touch a seat that has since gone to someone else.
            await conn.execute(
                "UPDATE seats SET status = 'available', user_id = NULL, reservation_id = NULL "
                "WHERE show_id = %s AND reservation_id = %s",
                (row["show_id"], reservation_id),
            )
            await conn.execute(
                "UPDATE reservations SET status = 'cancelled' WHERE id = %s", (reservation_id,)
            )
            row["status"] = "cancelled"
            return _reservation_body(row), True


# ----------------------------------------------------------------------------- metrics support
async def seat_counts_for_metrics(limit: int) -> list[dict]:
    async with db.pool.connection(timeout=2) as conn:
        cur = await conn.execute(
            "WITH recent AS (SELECT id FROM shows ORDER BY created_at DESC LIMIT %s) "
            "SELECT s.show_id, "
            "  count(*) FILTER (WHERE s.status = 'available') AS available, "
            "  count(*) FILTER (WHERE s.status = 'held')      AS held, "
            "  count(*) FILTER (WHERE s.status = 'confirmed') AS confirmed "
            "FROM seats s JOIN recent r ON r.id = s.show_id GROUP BY s.show_id",
            (limit,),
        )
        return await cur.fetchall()
