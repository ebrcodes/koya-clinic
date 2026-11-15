"""Clinic booking API.

Levels covered:
  1  list slots, book one, look up a booking, reset
  2  API key on every API request, validation of booking details
  3  no double booking (database enforced), cancellation
  4  idempotent retries, cursor pagination of a person's bookings
  5  rate limit of 5 booking attempts per email per minute

All state lives in Postgres, so it survives restarts and works with several
server processes. The important guarantees are enforced by the database, not
by in memory checks.
"""
import base64
import hashlib
import hmac
import json
import math
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

API_KEY = os.environ.get("API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
POOL_MAX = int(os.environ.get("POOL_MAX", "10"))
SEED_DEMO = os.environ.get("SEED_DEMO", "true").lower() != "false"

RATE_LIMIT_ATTEMPTS = 5
RATE_LIMIT_WINDOW_SECONDS = 60
MAX_LIMIT = 50
DEFAULT_LIMIT = 10

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PUBLIC_PATHS = {"/", "/health"}
PUBLIC_PREFIXES = ("/static/",)  # the page's own font files, which cannot send the key
STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML = STATIC_DIR / "index.html"

SCHEMA = """
CREATE TABLE IF NOT EXISTS slots (
    id         text PRIMARY KEY,
    provider   text NOT NULL,
    start_text text NOT NULL,
    end_text   text NOT NULL,
    start_ts   timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS bookings (
    seq        bigserial PRIMARY KEY,
    id         text NOT NULL UNIQUE,
    slot_id    text NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
    name       text NOT NULL,
    email      text NOT NULL,
    status     text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
-- The core guarantee: at most one confirmed booking per slot, enforced by the
-- database so it holds even when requests arrive at the same instant.
CREATE UNIQUE INDEX IF NOT EXISTS one_confirmed_booking_per_slot
    ON bookings (slot_id) WHERE status = 'confirmed';
CREATE INDEX IF NOT EXISTS bookings_by_email ON bookings (email, seq);
CREATE TABLE IF NOT EXISTS idempotency (
    key        text PRIMARY KEY,
    body_hash  text NOT NULL,
    status     integer NOT NULL,
    response   jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS attempts (
    id    bigserial PRIMARY KEY,
    email text NOT NULL,
    at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS attempts_by_email ON attempts (email, at);
"""

pool: ConnectionPool | None = None


# ---------------------------------------------------------------- helpers

def reply(status, payload, headers=None):
    return JSONResponse(payload, status_code=status, headers=headers)


def error(status, message, headers=None):
    return reply(status, {"error": message}, headers)


def iso_ms(dt):
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def parse_ts(value):
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def booking_json(row):
    return {
        "id": row["id"],
        "slot_id": row["slot_id"],
        "name": row["name"],
        "email": row["email"],
        "status": row["status"],
        "created_at": iso_ms(row["created_at"]),
    }


def is_text(value):
    return isinstance(value, str) and value.strip() != ""


async def read_object(request):
    try:
        data = await request.json()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def init_db():
    with pool.connection() as conn:
        # Fast path: the schema is created in one transaction, so if the last
        # table exists they all do. Many cold starts at once then skip the lock
        # and the DDL entirely instead of queueing behind each other.
        done = conn.execute("SELECT to_regclass('public.attempts') IS NOT NULL AS ok").fetchone()
        if done["ok"]:
            return
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(727274)")
            cur.execute(SCHEMA)
            if SEED_DEMO:
                cur.execute("SELECT count(*) AS n FROM slots")
                if cur.fetchone()["n"] == 0:
                    seed_demo_slots(cur)


def seed_demo_slots(cur):
    """Upcoming slots so the web page is usable before any reset.

    The two providers start at different times, like a real rota.
    """
    base = datetime.now(timezone.utc).replace(hour=9, minute=0, second=0, microsecond=0)
    base += timedelta(days=1)
    rota = [("Dr Ade", 0, 7), ("Dr Bello", 2, 7)]  # name, first slot offset, slot count
    n = 0
    for day in range(5):
        for provider, offset, count in rota:
            for k in range(count):
                start = base + timedelta(days=day, minutes=30 * (offset + k))
                end = start + timedelta(minutes=30)
                n += 1
                cur.execute(
                    "INSERT INTO slots (id, provider, start_text, end_text, start_ts) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (f"s{n}", provider,
                     start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                     end.strftime("%Y-%m-%dT%H:%M:%SZ"), start),
                )


@asynccontextmanager
async def lifespan(_app):
    global pool
    if not API_KEY:
        raise RuntimeError("API_KEY environment variable is required")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required")
    pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=POOL_MAX,
        timeout=30,
        # Serverless hosts freeze the app between requests and the database may
        # sleep, so test each connection before handing it out.
        check=ConnectionPool.check_connection,
        # prepare_threshold=None keeps this working behind connection poolers
        kwargs={"row_factory": dict_row, "prepare_threshold": None, "connect_timeout": 15},
        open=False,
    )
    pool.open(wait=True, timeout=30)
    await run_in_threadpool(init_db)
    yield
    pool.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


# --------------------------------------------------------------- security

@app.middleware("http")
async def require_api_key(request: Request, call_next):
    path = request.url.path
    if (request.method == "OPTIONS" or path in PUBLIC_PATHS
            or path.startswith(PUBLIC_PREFIXES)):
        return await call_next(request)
    supplied = request.headers.get("x-api-key", "")
    if not hmac.compare_digest(supplied.encode(), API_KEY.encode()):
        return error(401, "missing or invalid API key")
    return await call_next(request)


# CORS is added after the key check so it wraps it and answers preflights first.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)


@app.exception_handler(StarletteHTTPException)
async def http_error(_request, exc):
    messages = {404: "not found", 405: "method not allowed"}
    return error(exc.status_code, messages.get(exc.status_code, str(exc.detail)))


@app.exception_handler(Exception)
async def unexpected_error(_request, _exc):
    return error(500, "internal server error")


# ------------------------------------------------------------------ pages

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", include_in_schema=False)
def index():
    html = INDEX_HTML.read_text(encoding="utf-8")
    safe_key = json.dumps(API_KEY).replace("<", "\\u003c")
    return HTMLResponse(html.replace("__API_KEY__", safe_key))


@app.get("/health", include_in_schema=False)
def health():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


# ------------------------------------------------------------------ slots

@app.post("/reset")
async def reset(request: Request):
    body = await read_object(request)
    slots = body.get("slots") if body else None
    if not isinstance(slots, list):
        return error(400, "slots must be a list")
    rows, seen = [], set()
    for item in slots:
        if not isinstance(item, dict):
            return error(400, "each slot must be an object")
        if not all(is_text(item.get(k)) for k in ("id", "provider", "start", "end")):
            return error(400, "each slot needs id, provider, start and end")
        try:
            start_ts = parse_ts(item["start"])
            parse_ts(item["end"])
        except ValueError:
            return error(400, "start and end must be ISO 8601 timestamps")
        if item["id"] in seen:
            return error(400, "slot ids must be unique")
        seen.add(item["id"])
        rows.append((item["id"], item["provider"], item["start"], item["end"], start_ts))

    def work():
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM bookings")
                cur.execute("DELETE FROM slots")
                cur.execute("DELETE FROM idempotency")
                cur.execute("DELETE FROM attempts")
                if rows:
                    cur.executemany(
                        "INSERT INTO slots (id, provider, start_text, end_text, start_ts) "
                        "VALUES (%s, %s, %s, %s, %s)", rows)

    await run_in_threadpool(work)
    return reply(200, {"slots": len(rows)})


@app.get("/slots")
async def list_slots(request: Request):
    provider = request.query_params.get("provider")

    def work():
        sql = (
            "SELECT s.id, s.provider, s.start_text, s.end_text, "
            "NOT EXISTS (SELECT 1 FROM bookings b "
            "            WHERE b.slot_id = s.id AND b.status = 'confirmed') AS available "
            "FROM slots s "
        )
        params = []
        if provider is not None:
            sql += "WHERE s.provider = %s "
            params.append(provider)
        sql += "ORDER BY s.start_ts, s.id"
        with pool.connection() as conn:
            return conn.execute(sql, params).fetchall()

    rows = await run_in_threadpool(work)
    return reply(200, {"slots": [
        {"id": r["id"], "provider": r["provider"], "start": r["start_text"],
         "end": r["end_text"], "available": r["available"]} for r in rows]})


# --------------------------------------------------------------- bookings

def lock(cur, name):
    """Transaction scoped lock on an arbitrary string, released at commit."""
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 1))", (name,))


def check_rate_limit(cur, email_key):
    """Allow at most 5 attempts per email in any 60 second window.

    Returns None if allowed (and records the attempt), otherwise the number of
    seconds to wait. The per email lock makes the count and the insert atomic,
    so simultaneous requests cannot slip past the limit.
    """
    lock(cur, f"rl:{email_key}")
    cur.execute(
        "DELETE FROM attempts WHERE email = %s "
        "AND at < clock_timestamp() - make_interval(secs => %s)",
        (email_key, RATE_LIMIT_WINDOW_SECONDS))
    cur.execute(
        "SELECT count(*) AS n, "
        "EXTRACT(EPOCH FROM (min(at) + make_interval(secs => %s) - clock_timestamp())) AS wait "
        "FROM attempts WHERE email = %s",
        (RATE_LIMIT_WINDOW_SECONDS, email_key))
    row = cur.fetchone()
    if row["n"] >= RATE_LIMIT_ATTEMPTS:
        return max(1, math.ceil(float(row["wait"])))
    cur.execute("INSERT INTO attempts (email) VALUES (%s)", (email_key,))
    return None


def attempt_booking(conn, cur, body):
    """Run one booking attempt. Returns (status, payload, headers)."""
    raw_email = body.get("email")
    email_key = raw_email.strip().lower() if is_text(raw_email) else None

    if email_key:
        wait = check_rate_limit(cur, email_key)
        if wait is not None:
            return (429, {"error": "too many booking attempts for this email, try again later"},
                    {"Retry-After": str(wait)})

    slot_id, name = body.get("slot_id"), body.get("name")
    if not is_text(slot_id):
        return 400, {"error": "slot_id is required"}, {}
    if not is_text(name):
        return 400, {"error": "name is required"}, {}
    if not is_text(raw_email):
        return 400, {"error": "email is required"}, {}
    name = name.strip()
    if len(name) > 200:
        return 400, {"error": "name is too long"}, {}
    if len(email_key) > 254 or not EMAIL_RE.match(email_key):
        return 400, {"error": "email is not valid"}, {}

    cur.execute("SELECT start_ts <= now() AS started FROM slots WHERE id = %s", (slot_id,))
    slot = cur.fetchone()
    if slot is None:
        return 404, {"error": "slot not found"}, {}
    if slot["started"]:
        return 400, {"error": "slot has already started"}, {}

    try:
        # Savepoint, so a clash does not abort the surrounding transaction.
        with conn.transaction():
            cur.execute(
                "INSERT INTO bookings (id, slot_id, name, email, status) "
                "VALUES (%s, %s, %s, %s, 'confirmed') "
                "RETURNING id, slot_id, name, email, status, created_at",
                (uuid.uuid4().hex[:16], slot_id, name, email_key))
            booking = cur.fetchone()
    except UniqueViolation:
        return 409, {"error": "slot already booked"}, {}
    return 201, booking_json(booking), {}


def create_booking(body, idem_key):
    body_hash = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            if idem_key:
                # Serialise requests that share a key, so a retry that races the
                # original waits for it and then replays its stored response.
                lock(cur, f"idem:{idem_key}")
                cur.execute("SELECT body_hash, status, response FROM idempotency "
                            "WHERE key = %s", (idem_key,))
                saved = cur.fetchone()
                if saved:
                    if saved["body_hash"] != body_hash:
                        return (422, {"error": "Idempotency-Key was already used "
                                               "with a different request body"}, {})
                    return saved["status"], saved["response"], {}
            status, payload, headers = attempt_booking(conn, cur, body)
            # Rate limit answers are not stored: a later retry should be judged afresh.
            if idem_key and status != 429:
                cur.execute(
                    "INSERT INTO idempotency (key, body_hash, status, response) "
                    "VALUES (%s, %s, %s, %s::jsonb)",
                    (idem_key, body_hash, status, json.dumps(payload)))
            return status, payload, headers


@app.post("/bookings")
async def post_booking(request: Request):
    body = await read_object(request)
    if body is None:
        return error(400, "request body must be a JSON object")
    idem_key = (request.headers.get("idempotency-key") or "").strip() or None
    if idem_key and len(idem_key) > 255:
        return error(400, "Idempotency-Key is too long")
    status, payload, headers = await run_in_threadpool(create_booking, body, idem_key)
    return reply(status, payload, headers)


@app.get("/bookings")
async def list_bookings(request: Request):
    params = request.query_params
    email = (params.get("email") or "").strip().lower()
    if not email:
        return error(400, "email is required")

    limit = DEFAULT_LIMIT
    if "limit" in params:
        raw = params["limit"].strip()
        if not re.fullmatch(r"\d+", raw) or not 1 <= int(raw) <= MAX_LIMIT:
            return error(400, f"limit must be a whole number from 1 to {MAX_LIMIT}")
        limit = int(raw)

    after = 0
    if params.get("cursor"):
        try:
            after = int(base64.urlsafe_b64decode(params["cursor"].encode()).decode())
        except Exception:
            return error(400, "cursor is not valid")

    def work():
        with pool.connection() as conn:
            return conn.execute(
                "SELECT seq, id, slot_id, name, email, status, created_at FROM bookings "
                "WHERE email = %s AND seq > %s ORDER BY seq LIMIT %s",
                (email, after, limit + 1)).fetchall()

    rows = await run_in_threadpool(work)
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        next_cursor = base64.urlsafe_b64encode(str(page[-1]["seq"]).encode()).decode()
    return reply(200, {"bookings": [booking_json(r) for r in page],
                       "next_cursor": next_cursor})


@app.get("/bookings/{booking_id}")
async def get_booking(booking_id: str):
    def work():
        with pool.connection() as conn:
            return conn.execute(
                "SELECT id, slot_id, name, email, status, created_at FROM bookings "
                "WHERE id = %s", (booking_id,)).fetchone()

    row = await run_in_threadpool(work)
    if row is None:
        return error(404, "booking not found")
    return reply(200, booking_json(row))


@app.delete("/bookings/{booking_id}")
async def cancel_booking(booking_id: str):
    def work():
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # FOR UPDATE makes two simultaneous cancels take turns.
                cur.execute(
                    "SELECT id, slot_id, name, email, status, created_at FROM bookings "
                    "WHERE id = %s FOR UPDATE", (booking_id,))
                row = cur.fetchone()
                if row is None:
                    return 404, {"error": "booking not found"}
                if row["status"] == "cancelled":
                    return 409, {"error": "booking already cancelled"}
                cur.execute("UPDATE bookings SET status = 'cancelled' WHERE id = %s",
                            (booking_id,))
                row["status"] = "cancelled"
                return 200, booking_json(row)

    status, payload = await run_in_threadpool(work)
    return reply(status, payload)