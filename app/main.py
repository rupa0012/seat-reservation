"""HTTP layer: validation, auth, metrics, health. All booking decisions are in service.py."""
import asyncio
import hmac
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Optional

import psycopg
from fastapi import Depends, FastAPI, Header, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field, StringConstraints, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import config, db, metrics, service
from .logging_conf import log_fields_var, request_id_var, setup_logging

setup_logging(config.LOG_LEVEL)
log = logging.getLogger("app")

state = {"schema_ready": False}


# ----------------------------------------------------------------------------- lifespan
async def _init_schema_forever() -> None:
    """Keep trying until the DB is reachable, so a cold start never crashes the container."""
    delay = 0.5
    while True:
        try:
            await db.init_schema()
            state["schema_ready"] = True
            log.info("schema ready")
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("schema init failed, retrying", extra={"fields": {"error": str(exc)}})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.pool.open(wait=False)
    task = asyncio.create_task(_init_schema_forever())
    log.info("started", extra={"fields": {"pool_max": config.POOL_MAX}})
    yield
    task.cancel()
    await db.pool.close()


app = FastAPI(title="Seat Reservation", version="1.0.0", lifespan=lifespan)


# ----------------------------------------------------------------------------- request id + access log
_RID_OK = re.compile(r"[A-Za-z0-9._-]{1,64}")


class RequestContextMiddleware:
    """Pure-ASGI middleware: assigns a request id, logs one JSON line per request, counts metrics."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = dict(scope["headers"]).get(b"x-request-id", b"").decode("latin-1")
        rid = incoming if _RID_OK.fullmatch(incoming) else uuid.uuid4().hex
        rid_token = request_id_var.set(rid)
        fields: dict = {}
        fields_token = log_fields_var.set(fields)
        started = time.perf_counter()
        status = 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers") or [])
                headers.append((b"x-request-id", rid.encode()))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - started
            route = getattr(scope.get("route"), "path", "unmatched")
            metrics.HTTP_REQUESTS.labels(scope["method"], route, str(status)).inc()
            metrics.HTTP_LATENCY.labels(route).observe(duration)
            if route not in ("/health", "/metrics"):
                log.info(
                    "request",
                    extra={
                        "fields": {
                            "method": scope["method"],
                            "path": scope["path"],
                            "status": status,
                            "duration_ms": round(duration * 1000, 2),
                            **fields,
                        }
                    },
                )
            request_id_var.reset(rid_token)
            log_fields_var.reset(fields_token)


app.add_middleware(RequestContextMiddleware)


# ----------------------------------------------------------------------------- errors
class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, headers: Optional[dict] = None):
        self.status, self.code, self.message, self.headers = status, code, message, headers or {}


@app.exception_handler(ApiError)
async def _api_error(_: Request, exc: ApiError):
    return JSONResponse({"error": exc.code, "message": exc.message}, status_code=exc.status, headers=exc.headers)


@app.exception_handler(RequestValidationError)
async def _validation_error(_: Request, exc: RequestValidationError):
    return JSONResponse(
        {"error": "validation_error", "details": jsonable_encoder(exc.errors())}, status_code=422
    )


@app.exception_handler(StarletteHTTPException)
async def _http_error(_: Request, exc: StarletteHTTPException):
    return JSONResponse({"error": "http_error", "message": str(exc.detail)}, status_code=exc.status_code)


@app.exception_handler(PoolTimeout)
@app.exception_handler(psycopg.OperationalError)
@app.exception_handler(psycopg.errors.QueryCanceled)
async def _db_unavailable(_: Request, exc: Exception):
    log.error("database unavailable", extra={"fields": {"error": type(exc).__name__}})
    return JSONResponse(
        {"error": "service_unavailable", "message": "database is busy or unreachable, retry shortly"},
        status_code=503,
        headers={"Retry-After": "1"},
    )


@app.exception_handler(Exception)
async def _unexpected(_: Request, exc: Exception):
    log.exception("unhandled error")
    return JSONResponse({"error": "internal_error", "message": "unexpected error"}, status_code=500)


# ----------------------------------------------------------------------------- auth
# Demo auth: "Authorization: Bearer <token>" where the token IS the user's identity.
# Identity always comes from here, never from the request body. A real deployment would
# verify a signed token (JWT) and take `sub` from it; nothing else in the code would change.
_USER_RE = re.compile(r"[A-Za-z0-9_.:@-]{1,64}")


# Declaring the scheme with HTTPBearer makes Swagger UI (/docs) show an "Authorize" button.
# (Swagger UI silently drops a plain header parameter named "Authorization".)
_bearer_scheme = HTTPBearer(auto_error=False)


def _bearer(creds: Optional[HTTPAuthorizationCredentials]) -> Optional[str]:
    if creds is None or creds.scheme.lower() != "bearer":
        return None
    token = creds.credentials.strip()
    return token or None


def get_user(creds: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer_scheme)] = None) -> str:
    token = _bearer(creds)
    unauthorized = ApiError(401, "unauthorized", "send 'Authorization: Bearer <token>'", {"WWW-Authenticate": "Bearer"})
    if token is None or not _USER_RE.fullmatch(token):
        raise unauthorized
    if hmac.compare_digest(token, config.ADMIN_TOKEN):
        return "admin"
    log_fields_var.get()["user_id"] = token
    return token


def require_admin(creds: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer_scheme)] = None) -> None:
    token = _bearer(creds)
    if token is None:
        raise ApiError(401, "unauthorized", "admin token required", {"WWW-Authenticate": "Bearer"})
    if not hmac.compare_digest(token, config.ADMIN_TOKEN):
        raise ApiError(403, "forbidden", "admin only")


# ----------------------------------------------------------------------------- models
SeatLabel = Annotated[str, StringConstraints(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9_.-]+$")]


class CreateShow(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    seats: list[SeatLabel] = Field(min_length=1, max_length=config.MAX_SEATS_PER_SHOW)
    price_paise: int = Field(strict=True, ge=0)  # strict: 250.5 or "250" are rejected, money is integer paise
    per_user_limit: int = Field(default=config.DEFAULT_PER_USER_LIMIT, strict=True, ge=1, le=100)

    @field_validator("seats")
    @classmethod
    def _unique(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("seat labels must be unique")
        return v


class ReserveBody(BaseModel):
    # Unknown fields (e.g. a spoofed "user_id") are ignored by pydantic: identity is token-only.
    seats: list[SeatLabel] = Field(min_length=1, max_length=config.MAX_SEATS_PER_REQUEST)
    idempotency_key: Optional[str] = Field(default=None, min_length=1, max_length=255)

    @field_validator("seats")
    @classmethod
    def _unique(cls, v):
        if len(set(v)) != len(v):
            raise ValueError("seat labels must be unique within a request")
        return v


# ----------------------------------------------------------------------------- health + metrics
@app.get("/health")
async def health():
    """Liveness: the process is up. Deliberately does not touch the database."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness: fails closed (503) unless the database answers right now and the schema exists."""
    try:
        async with db.pool.connection(timeout=2) as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"status": "unavailable", "reason": "database_unreachable"}, status_code=503)
    if not state["schema_ready"]:
        return JSONResponse({"status": "unavailable", "reason": "schema_not_ready"}, status_code=503)
    return {"status": "ready"}


@app.get("/metrics")
async def metrics_endpoint():
    out = generate_latest(metrics.registry).decode()
    # Seat gauges come straight from Postgres at scrape time, so they always match the API state.
    try:
        rows = await service.seat_counts_for_metrics(config.MAX_SHOWS_IN_METRICS)
        lines = [
            "# HELP seats_available Seats currently available, read from the database at scrape time.",
            "# TYPE seats_available gauge",
        ]
        lines += [f'seats_available{{show_id="{r["show_id"]}"}} {r["available"]}' for r in rows]
        lines += [
            "# HELP seats_by_status Seats per status, read from the database at scrape time.",
            "# TYPE seats_by_status gauge",
        ]
        for r in rows:
            for st in ("available", "held", "confirmed"):
                lines.append(f'seats_by_status{{show_id="{r["show_id"]}",status="{st}"}} {r[st]}')
        out += "\n".join(lines) + "\n"
    except Exception as exc:  # noqa: BLE001
        log.warning("seat gauges unavailable", extra={"fields": {"error": type(exc).__name__}})
    return Response(out, media_type=CONTENT_TYPE_LATEST)


# ----------------------------------------------------------------------------- API
@app.post("/shows", status_code=201)
async def create_show(body: CreateShow, _: None = Depends(require_admin)):
    show = await service.create_show(body.name, body.seats, body.price_paise, body.per_user_limit)
    return JSONResponse(show, status_code=201)


@app.get("/shows/{show_id}")
async def get_show(show_id: uuid.UUID, seats: bool = True):
    """Pass ?seats=false for counts only (cheaper on big halls)."""
    try:
        return await service.show_state(show_id, include_seats=seats)
    except service.ShowNotFound:
        raise ApiError(404, "show_not_found", "no such show")


@app.post("/shows/{show_id}/reserve")
async def reserve(
    show_id: uuid.UUID,
    body: ReserveBody,
    user_id: str = Depends(get_user),
    idempotency_key: Annotated[Optional[str], Header(alias="Idempotency-Key")] = None,
):
    key = idempotency_key or body.idempotency_key
    if not key or len(key) > 255:
        raise ApiError(422, "idempotency_key_required", "send an Idempotency-Key header or idempotency_key field")

    try:
        out = await service.reserve(show_id, user_id, key, body.seats)
    except service.ShowNotFound:
        raise ApiError(404, "show_not_found", "no such show")
    except service.UnknownSeat as e:
        raise ApiError(404, "unknown_seat", f"seat {e.label} does not exist in this show")

    if out.kind == "confirmed":
        metrics.RESERVATIONS_CONFIRMED.inc()
    else:
        metrics.RESERVATIONS_DECLINED.labels(reason=out.kind).inc()
    log_fields_var.get()["outcome"] = out.kind
    return JSONResponse(out.body, status_code=out.status_code, headers=out.headers)


@app.post("/reservations/{reservation_id}/cancel")
async def cancel_reservation(reservation_id: uuid.UUID, user_id: str = Depends(get_user)):
    try:
        body, released = await service.cancel(reservation_id, user_id)
    except service.ReservationNotFound:
        raise ApiError(404, "reservation_not_found", "no such reservation")
    except service.NotOwner:
        raise ApiError(403, "forbidden", "only the owner can cancel a reservation")
    if released:
        metrics.RESERVATIONS_CANCELLED.inc()
    return body
