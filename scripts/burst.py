#!/usr/bin/env python3
"""On-sale stampede against a running instance, then reconcile.

    python scripts/burst.py http://localhost:8000
    python scripts/burst.py https://your-app.onrender.com --requests 20000

What it does (all against a FRESH show it creates, so it is repeatable):
  A. hot-seat storm   - many different users fight for each of a few hot seats
  B. on-sale stampede - thousands of users grab random seats; ~10% send the same request twice at once
  C. per-user limit   - one user fires 10 parallel reserves (limit is 4)
  D. idempotency      - same key x20 in parallel, then same key with different seats
  E. identity         - spoofed user_id in the body is ignored; a stranger cannot cancel
Then it checks the invariants and the metrics, prints the outcome distribution and PASS/FAIL.
Exit code is 1 if anything failed.
"""
import argparse
import asyncio
import collections
import os
import random
import re
import sys
import time
import uuid

import httpx


PROGRESS = {"done": 0, "five_xx": 0, "transport": 0}


async def _ticker(t0):
    """Print a progress line every 5s so a slow run is visible instead of looking hung."""
    last = 0
    while True:
        await asyncio.sleep(5)
        done = PROGRESS["done"]
        print(f"   ... {done} requests done ({(done - last) / 5:.0f}/s), 5xx={PROGRESS['five_xx']}, transport_errors={PROGRESS['transport']}, elapsed {time.perf_counter() - t0:.0f}s", flush=True)
        last = done


class Stats:
    def __init__(self):
        self.dist = collections.Counter()         # "201 confirmed", "409 seat_taken", ...
        self.fresh_confirmed = []                 # (token, [seats]) for every NEW 201
        self.replays = 0
        self.five_xx = 0
        self.transport_errors = 0
        self.fresh_declines = collections.Counter()   # metric-style reason -> count (non-replay)
        self.winners = collections.defaultdict(list)  # hot seat -> tokens that got a 201 (phase A)


REASON_OF_ERROR = {
    "seat_taken": "seat-taken",
    "per_user_limit": "per-user-limit",
    "idempotency_key_reuse": "idempotency-key-conflict",
}


async def reserve(client, base, show_id, token, seats, key, stats, extra=None, hot_phase=False):
    body = {"seats": seats, "idempotency_key": key}
    if extra:
        body.update(extra)
    try:
        r = await client.post(
            f"{base}/shows/{show_id}/reserve", json=body, headers={"Authorization": f"Bearer {token}"}
        )
    except Exception:  # noqa: BLE001
        stats.transport_errors += 1
        PROGRESS["done"] += 1
        PROGRESS["transport"] += 1
        return None
    PROGRESS["done"] += 1
    if r.status_code >= 500:
        PROGRESS["five_xx"] += 1
    replay = r.headers.get("idempotent-replay") == "true"
    try:
        data = r.json()
    except Exception:  # noqa: BLE001
        data = {}
    if r.status_code >= 500:
        stats.five_xx += 1
        stats.dist[f"{r.status_code} server-error"] += 1
    elif replay:
        stats.replays += 1
        stats.dist[f"{r.status_code} idempotent-replay"] += 1
    elif r.status_code == 201:
        stats.dist["201 confirmed"] += 1
        stats.fresh_confirmed.append((token, list(data["seats"])))
        if hot_phase:
            for s in data["seats"]:
                stats.winners[s].append(token)
    else:
        err = data.get("error", "unknown")
        stats.dist[f"{r.status_code} {err}"] += 1
        stats.fresh_declines[REASON_OF_ERROR.get(err, err)] += 1
    return r.status_code, data, replay


def parse_metrics(text):
    out = {"confirmed": 0.0, "declined": collections.defaultdict(float)}
    for line in text.splitlines():
        if line.startswith("reservations_confirmed_total "):
            out["confirmed"] = float(line.split()[1])
        m = re.match(r'reservations_declined_total\{reason="([^"]+)"\} (\S+)', line)
        if m:
            out["declined"][m.group(1)] = float(m.group(2))
    return out


async def main(a):
    base = a.base_url.rstrip("/")
    run = uuid.uuid4().hex[:6]
    results = []  # (name, ok, detail)

    def check(name, ok, detail=""):
        results.append((name, bool(ok), detail))

    limits = httpx.Limits(max_connections=a.concurrency, max_keepalive_connections=a.concurrency)
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0), limits=limits) as client:
        # ---- readiness + show
        r = await client.get(f"{base}/ready")
        if r.status_code != 200:
            print(f"service not ready: {r.status_code} {r.text}")
            return 2
        hot = [f"H{i}" for i in range(1, a.hot_seats + 1)]
        regular = [f"S{i}" for i in range(1, a.seats + 1)]
        extra = ["E1", "E2"]
        limit_seats = [f"L{i}" for i in range(1, 11)]
        r = await client.post(
            f"{base}/shows",
            json={"name": f"burst-{run}", "seats": hot + regular + extra + limit_seats, "price_paise": 25000},
            headers={"Authorization": f"Bearer {a.admin_token}"},
        )
        if r.status_code != 201:
            print(f"could not create show: {r.status_code} {r.text}")
            return 2
        show = r.json()
        show_id, total = show["id"], show["total_seats"]
        limit = show["per_user_limit"]
        print(f"show {show_id}: {total} seats, per-user limit {limit}")

        m0 = parse_metrics((await client.get(f"{base}/metrics")).text)
        st = Stats()
        t0 = time.perf_counter()
        ticker = asyncio.create_task(_ticker(t0))

        # ---- A. hot-seat storm
        tasks = []
        for seat in hot:
            for u in range(a.hot_users):
                tasks.append(reserve(client, base, show_id, f"hot-{run}-{seat}-u{u}", [seat], f"k-{uuid.uuid4().hex}", st, hot_phase=True))
        random.shuffle(tasks)
        print(f"A. hot-seat storm: {len(tasks)} requests on {len(hot)} seats ...")
        await asyncio.gather(*tasks)

        # ---- B. on-sale stampede
        users = [f"buyer-{run}-{i}" for i in range(a.users)]
        tasks = []
        for _ in range(a.requests):
            token = random.choice(users)
            seats = random.sample(regular, random.choice([1, 1, 1, 2]))
            key = f"k-{uuid.uuid4().hex}"
            tasks.append(reserve(client, base, show_id, token, seats, key, st))
            if random.random() < 0.10:  # client retry racing the original
                tasks.append(reserve(client, base, show_id, token, seats, key, st))
        random.shuffle(tasks)
        print(f"B. on-sale stampede: {len(tasks)} requests ...")
        await asyncio.gather(*tasks)

        # ---- C. per-user limit
        c_user = f"limit-{run}"
        print("C. per-user limit: 10 parallel reserves from one user ...")
        c_stats = Stats()
        await asyncio.gather(*[reserve(client, base, show_id, c_user, [s], f"c-{uuid.uuid4().hex}", c_stats) for s in limit_seats])
        check(f"C1 one user, 10 parallel reserves -> exactly {limit} confirmed", len(c_stats.fresh_confirmed) == limit, f"confirmed={len(c_stats.fresh_confirmed)} dist={dict(c_stats.dist)}")
        # (merge into global stats so reconciliation sees them)
        for k, v in c_stats.dist.items():
            st.dist[k] += v
        st.fresh_confirmed += c_stats.fresh_confirmed
        st.fresh_declines.update(c_stats.fresh_declines)
        st.five_xx += c_stats.five_xx
        st.transport_errors += c_stats.transport_errors
        st.replays += c_stats.replays

        # ---- D. idempotency
        d_user = f"idem-{run}"
        d_seat = f"E1"
        print("D. idempotency: same key x20 in parallel, then same key with different seats ...")
        d_stats = Stats()
        key = f"d-{uuid.uuid4().hex}"
        outs = await asyncio.gather(*[reserve(client, base, show_id, d_user, [d_seat], key, d_stats) for _ in range(20)])
        ids = {o[1].get("reservation_id") for o in outs if o and o[0] == 201}
        codes = collections.Counter(o[0] for o in outs if o)
        diff = await reserve(client, base, show_id, d_user, ["E2"], key, d_stats)
        for k, v in d_stats.dist.items():
            st.dist[k] += v
        st.fresh_confirmed += d_stats.fresh_confirmed
        st.fresh_declines.update(d_stats.fresh_declines)
        st.five_xx += d_stats.five_xx
        st.transport_errors += d_stats.transport_errors
        st.replays += d_stats.replays
        check("D1 same key x20 -> exactly one reservation", len(ids) == 1 and codes == {201: 20}, f"ids={len(ids)} codes={dict(codes)}")
        check("D2 same key, different seats -> 409", diff and diff[0] == 409 and diff[1].get("error") == "idempotency_key_reuse", f"got {diff[0] if diff else None}")

        # ---- E. identity
        e_user = f"spoof-{run}"
        res = await reserve(client, base, show_id, e_user, ["E2"], f"e-{uuid.uuid4().hex}", st, extra={"user_id": "victim", "user": "victim"})
        spoof_ok = res and res[0] == 201 and res[1].get("user_id") == e_user
        check("E1 spoofed user_id in body is ignored", spoof_ok, f"response user_id={res[1].get('user_id') if res else None}")
        if res and res[0] == 201:
            rid = res[1]["reservation_id"]
            r1 = await client.post(f"{base}/reservations/{rid}/cancel", headers={"Authorization": f"Bearer stranger-{run}"})
            check("E2 stranger cannot cancel", r1.status_code == 403, f"status={r1.status_code}")
            r2 = await client.post(f"{base}/reservations/{rid}/cancel", headers={"Authorization": f"Bearer {e_user}"})
            check("E3 owner can cancel", r2.status_code == 200 and r2.json().get("status") == "cancelled", f"status={r2.status_code}")
            released_seat = res[1]["seats"][0]
            st.fresh_confirmed = [(t, [s for s in ss if not (t == e_user and s == released_seat)]) for t, ss in st.fresh_confirmed]
            st.fresh_confirmed = [(t, ss) for t, ss in st.fresh_confirmed if ss]
            r3 = await reserve(client, base, show_id, f"rebook-{run}", [released_seat], f"e-{uuid.uuid4().hex}", st)
            check("E4 released seat is re-bookable", r3 and r3[0] == 201, f"status={r3[0] if r3 else None}")

        elapsed = time.perf_counter() - t0
        ticker.cancel()

        # ---- reconciliation
        state = (await client.get(f"{base}/shows/{show_id}")).json()
        counts = state["counts"]
        m1 = parse_metrics((await client.get(f"{base}/metrics")).text)

    all_seats = [s for _, ss in st.fresh_confirmed for s in ss]
    per_user = collections.Counter()
    for tok, ss in st.fresh_confirmed:
        per_user[tok] += len(ss)

    check("1 hot seats: exactly one 201 each", all(len(st.winners[s]) == 1 for s in hot), {s: len(st.winners[s]) for s in hot})
    check("1b no seat confirmed twice (whole run)", len(all_seats) == len(set(all_seats)), f"{len(all_seats)} confirmations / {len(set(all_seats))} unique seats")
    check("2 zero 5xx and zero transport errors", st.five_xx == 0 and st.transport_errors == 0, f"5xx={st.five_xx} transport_errors={st.transport_errors}")
    check("3 invariant available+held+confirmed == total", sum(counts.values()) == total and state["reconciled"], f"{counts} total={total}")
    check("3b server confirmed == client-side tally", counts["confirmed"] == len(set(all_seats)), f"server={counts['confirmed']} client={len(set(all_seats))}")
    check(f"5 per-user limit holds (max {limit})", max(per_user.values(), default=0) <= limit and per_user[c_user] <= limit, f"max={max(per_user.values(), default=0)} limit-tester={per_user[c_user]}")
    n_fresh = len(st.fresh_confirmed)
    # metrics counters are process-wide: only comparable if nobody else hit the service meanwhile.
    check("M1 metric reservations_confirmed delta == fresh 201s (+1 cancelled-seat rebook included)", int(m1["confirmed"] - m0["confirmed"]) == n_fresh + 1, f"metric={int(m1['confirmed'] - m0['confirmed'])} client={n_fresh + 1}")
    for reason in ("seat-taken", "per-user-limit"):
        got = int(m1["declined"][reason] - m0["declined"][reason])
        check(f"M2 metric declined[{reason}] delta == client count", got == st.fresh_declines[reason], f"metric={got} client={st.fresh_declines[reason]}")
    got = int(m1["declined"]["idempotent-replay"] - m0["declined"]["idempotent-replay"])
    check("M3 metric declined[idempotent-replay] delta == client replays", got == st.replays, f"metric={got} client={st.replays}")

    print()
    print(f"=== outcome distribution ({sum(st.dist.values())} requests in {elapsed:.1f}s, {sum(st.dist.values()) / elapsed:.0f} req/s)")
    for k, v in sorted(st.dist.items(), key=lambda kv: -kv[1]):
        print(f"  {v:>7}  {k}")
    print()
    print(f"=== final reconciliation: {counts}  total_seats={total}  sum={sum(counts.values())}")
    print()
    print("=== checks")
    failed = 0
    for name, ok, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f"   -> {detail}"))
        failed += 0 if ok else 1
    print()
    print("ALL CHECKS PASSED" if not failed else f"{failed} CHECK(S) FAILED")
    return 1 if failed else 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("base_url", nargs="?", default=os.getenv("BASE_URL", "http://localhost:8000"))
    p.add_argument("--admin-token", default=os.getenv("ADMIN_TOKEN", "admin-secret"))
    p.add_argument("--requests", type=int, default=15000, help="phase B request count (default 15000)")
    p.add_argument("--hot-seats", type=int, default=5)
    p.add_argument("--hot-users", type=int, default=400, help="contenders per hot seat")
    p.add_argument("--seats", type=int, default=1000, help="regular seats in the hall")
    p.add_argument("--users", type=int, default=3000, help="distinct buyers in phase B")
    p.add_argument("--concurrency", type=int, default=500, help="max simultaneous connections from this client")
    sys.exit(asyncio.run(main(p.parse_args())))
