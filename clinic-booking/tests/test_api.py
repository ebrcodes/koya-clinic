"""End to end tests, run against a live server.

    BASE_URL=http://localhost:8000 API_KEY=... pytest -q

The server should run with more than one worker so the concurrency tests prove
the guarantees hold across processes.
"""
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest

BASE = os.environ.get("BASE_URL", "http://localhost:8000")
KEY = os.environ.get("API_KEY", "test-key-123")
H = {"X-API-Key": KEY}


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


NOW = datetime.now(timezone.utc)
FUTURE = NOW + timedelta(days=3)
SLOTS = [
    {"id": "s1", "provider": "Dr Ade", "start": iso(FUTURE.replace(hour=9, minute=0, second=0)),
     "end": iso(FUTURE.replace(hour=9, minute=30, second=0))},
    {"id": "s2", "provider": "Dr Ade", "start": iso(FUTURE.replace(hour=8, minute=0, second=0)),
     "end": iso(FUTURE.replace(hour=8, minute=30, second=0))},
    {"id": "s3", "provider": "Dr Bello", "start": iso(FUTURE.replace(hour=10, minute=0, second=0)),
     "end": iso(FUTURE.replace(hour=10, minute=30, second=0))},
    {"id": "past", "provider": "Dr Ade", "start": iso(NOW - timedelta(days=1)),
     "end": iso(NOW - timedelta(days=1) + timedelta(minutes=30))},
] + [{"id": f"m{i}", "provider": "Dr Many",
      "start": iso(FUTURE + timedelta(days=1, minutes=30 * i)),
      "end": iso(FUTURE + timedelta(days=1, minutes=30 * i + 30))} for i in range(12)]


@pytest.fixture(autouse=True)
def fresh():
    r = httpx.post(f"{BASE}/reset", headers=H, json={"slots": SLOTS})
    assert r.status_code == 200, r.text


def post(body, headers=None, **kw):
    return httpx.post(f"{BASE}/bookings", headers={**H, **(headers or {})}, json=body, **kw)


def book(slot="s1", name="Ada Obi", email="ada@example.com", **kw):
    return post({"slot_id": slot, "name": name, "email": email}, **kw)


# ---------------------------------------------------------------- level 1

def test_slots_sorted_earliest_first_with_availability():
    r = httpx.get(f"{BASE}/slots", headers=H)
    assert r.status_code == 200
    slots = r.json()["slots"]
    starts = [s["start"] for s in slots]
    assert starts == sorted(starts)
    assert set(slots[0]) == {"id", "provider", "start", "end", "available"}
    assert all(s["available"] for s in slots)


def test_slots_provider_filter_is_exact():
    ids = {s["id"] for s in httpx.get(f"{BASE}/slots?provider=Dr Ade", headers=H).json()["slots"]}
    assert ids == {"s1", "s2", "past"}
    assert httpx.get(f"{BASE}/slots?provider=dr ade", headers=H).json()["slots"] == []


def test_book_and_get_and_availability_flips():
    r = book()
    assert r.status_code == 201
    b = r.json()
    assert set(b) == {"id", "slot_id", "name", "email", "status", "created_at"}
    assert b["status"] == "confirmed" and b["slot_id"] == "s1"
    assert b["created_at"].endswith("Z")
    got = httpx.get(f"{BASE}/bookings/{b['id']}", headers=H)
    assert got.status_code == 200 and got.json() == b
    slots = {s["id"]: s for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]}
    assert slots["s1"]["available"] is False and slots["s2"]["available"] is True


def test_unknown_booking_404():
    r = httpx.get(f"{BASE}/bookings/nope", headers=H)
    assert r.status_code == 404 and "error" in r.json()


def test_reset_clears_bookings_and_replaces_slots():
    book()
    httpx.post(f"{BASE}/reset", headers=H, json={"slots": SLOTS[:1]})
    assert [s["id"] for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]] == ["s1"]
    assert httpx.get(f"{BASE}/bookings?email=ada@example.com", headers=H).json()["bookings"] == []


# ---------------------------------------------------------------- level 2

@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
def test_api_key_required_everywhere(headers):
    for method, path in [("GET", "/slots"), ("POST", "/bookings"), ("GET", "/bookings/x"),
                         ("DELETE", "/bookings/x"), ("GET", "/bookings?email=a@b.co"),
                         ("POST", "/reset")]:
        r = httpx.request(method, f"{BASE}{path}", headers=headers, json={})
        assert r.status_code == 401, (method, path)
        assert "error" in r.json()


@pytest.mark.parametrize("body", [
    {}, {"name": "A", "email": "a@b.co"}, {"slot_id": "s1", "email": "a@b.co"},
    {"slot_id": "s1", "name": "A"}, {"slot_id": "", "name": "A", "email": "a@b.co"},
    {"slot_id": "s1", "name": "   ", "email": "a@b.co"}, {"slot_id": "s1", "name": "A", "email": ""},
    {"slot_id": "s1", "name": "A", "email": "not-an-email"},
    {"slot_id": "s1", "name": "A", "email": "a@b"}, {"slot_id": "s1", "name": 5, "email": "a@b.co"},
    {"slot_id": "s1", "name": "A", "email": None},
])
def test_invalid_details_400(body):
    r = post(body)
    assert r.status_code == 400 and "error" in r.json()


def test_bad_json_400():
    r = httpx.post(f"{BASE}/bookings", headers={**H, "Content-Type": "application/json"}, content="{nope")
    assert r.status_code == 400


def test_unknown_slot_404_and_past_slot_400():
    assert book(slot="zzz").status_code == 404
    assert book(slot="past").status_code == 400


def test_trims_and_lowercases():
    b = book(name="  Ada Obi  ", email="  ADA@Example.COM ").json()
    assert b["name"] == "Ada Obi" and b["email"] == "ada@example.com"


# ---------------------------------------------------------------- level 3

def test_second_booking_409_even_for_same_person():
    assert book().status_code == 201
    assert book(email="other@example.com").status_code == 409
    assert book().status_code == 409


def test_simultaneous_bookings_exactly_one_wins():
    def attempt(i):
        return book(slot="s2", name=f"P{i}", email=f"p{i}@example.com").status_code

    with ThreadPoolExecutor(max_workers=30) as ex:
        codes = list(ex.map(attempt, range(30)))
    assert codes.count(201) == 1, codes
    assert codes.count(409) == 29, codes
    bookings = [b for i in range(30) for b in httpx.get(
        f"{BASE}/bookings?email=p{i}@example.com", headers=H).json()["bookings"]]
    assert len(bookings) == 1


def test_cancel_frees_slot_and_allows_rebook():
    b = book().json()
    r = httpx.delete(f"{BASE}/bookings/{b['id']}", headers=H)
    assert r.status_code == 200 and r.json()["status"] == "cancelled" and r.json()["id"] == b["id"]
    assert httpx.get(f"{BASE}/bookings/{b['id']}", headers=H).json()["status"] == "cancelled"
    assert {s["id"]: s for s in httpx.get(f"{BASE}/slots", headers=H).json()["slots"]}["s1"]["available"]
    assert book(email="next@example.com").status_code == 201


def test_cancel_twice_409_and_unknown_404():
    b = book().json()
    assert httpx.delete(f"{BASE}/bookings/{b['id']}", headers=H).status_code == 200
    assert httpx.delete(f"{BASE}/bookings/{b['id']}", headers=H).status_code == 409
    assert httpx.delete(f"{BASE}/bookings/nope", headers=H).status_code == 404


def test_simultaneous_cancels_one_wins():
    b = book().json()
    with ThreadPoolExecutor(max_workers=10) as ex:
        codes = list(ex.map(lambda _: httpx.delete(f"{BASE}/bookings/{b['id']}", headers=H).status_code, range(10)))
    assert codes.count(200) == 1 and codes.count(409) == 9


# ---------------------------------------------------------------- level 4

def test_idempotent_replay_same_response():
    k = {"Idempotency-Key": str(uuid.uuid4())}
    a, b = book(headers=k), book(headers=k)
    assert a.status_code == b.status_code == 201
    assert a.json() == b.json()
    assert len(httpx.get(f"{BASE}/bookings?email=ada@example.com", headers=H).json()["bookings"]) == 1


def test_idempotent_replay_of_failure_is_same_failure():
    assert book(email="x@example.com").status_code == 201
    k = {"Idempotency-Key": str(uuid.uuid4())}
    assert book(email="y@example.com", headers=k).status_code == 409
    httpx.delete(f"{BASE}/bookings/{httpx.get(f'{BASE}/bookings?email=x@example.com', headers=H).json()['bookings'][0]['id']}", headers=H)
    assert book(email="y@example.com", headers=k).status_code == 409  # replay, not a fresh attempt


def test_same_key_different_body_422():
    k = {"Idempotency-Key": str(uuid.uuid4())}
    assert book(headers=k).status_code == 201
    r = book(slot="s2", headers=k)
    assert r.status_code == 422 and "error" in r.json()


def test_simultaneous_retries_with_same_key_create_one_booking():
    k = {"Idempotency-Key": str(uuid.uuid4())}
    with ThreadPoolExecutor(max_workers=20) as ex:
        rs = list(ex.map(lambda _: book(headers=k), range(20)))
    assert {r.status_code for r in rs} == {201}
    assert len({r.json()["id"] for r in rs}) == 1


def test_no_key_means_no_dedup():
    assert book().status_code == 201
    assert book().status_code == 409


def test_pagination_oldest_first_walks_everything_once():
    ids = []
    # 5 bookings: the most one email can make per minute (level 5)
    for i in range(5):
        ids.append(book(slot=f"m{i}", email="Many@Example.com", name=f"N{i}").json()["id"])
    seen, cursor, pages = [], None, 0
    while True:
        url = f"{BASE}/bookings?email=MANY@example.com&limit=2" + (f"&cursor={cursor}" if cursor else "")
        r = httpx.get(url, headers=H)
        assert r.status_code == 200
        data = r.json()
        seen += [b["id"] for b in data["bookings"]]
        pages += 1
        cursor = data["next_cursor"]
        if cursor is None:
            break
    assert seen == ids and pages == 3  # pages of 2, 2, 1


def test_pagination_defaults_and_limits():
    book()
    r = httpx.get(f"{BASE}/bookings?email=ada@example.com", headers=H)
    assert r.status_code == 200 and r.json()["next_cursor"] is None
    for bad in ["0", "51", "-1", "abc", "1.5", ""]:
        assert httpx.get(f"{BASE}/bookings?email=a@b.co&limit={bad}", headers=H).status_code == 400, bad
    assert httpx.get(f"{BASE}/bookings?email=a@b.co&limit=50", headers=H).status_code == 200
    assert httpx.get(f"{BASE}/bookings?email=a@b.co&limit=1", headers=H).status_code == 200
    assert httpx.get(f"{BASE}/bookings", headers=H).status_code == 400
    assert httpx.get(f"{BASE}/bookings?email=a@b.co&cursor=%%%", headers=H).status_code == 400


def test_exact_page_boundary_has_null_cursor():
    for i in range(4):
        book(slot=f"m{i}", email="edge@example.com")
    r = httpx.get(f"{BASE}/bookings?email=edge@example.com&limit=4", headers=H).json()
    assert len(r["bookings"]) == 4 and r["next_cursor"] is None


def test_listing_includes_cancelled_and_isolates_people():
    a = book(email="a@example.com").json()
    book(slot="s2", email="b@example.com")
    httpx.delete(f"{BASE}/bookings/{a['id']}", headers=H)
    got = httpx.get(f"{BASE}/bookings?email=A@EXAMPLE.COM", headers=H).json()["bookings"]
    assert [x["status"] for x in got] == ["cancelled"]


# ---------------------------------------------------------------- level 5

def test_sixth_attempt_per_email_is_429_with_retry_after():
    codes = []
    for _ in range(5):
        codes.append(book(slot="past", email="spam@example.com").status_code)  # failed attempts count
    assert codes == [400] * 5
    r = book(slot="s1", email="spam@example.com")
    assert r.status_code == 429 and "error" in r.json()
    ra = r.headers["Retry-After"]
    assert ra.isdigit() and 1 <= int(ra) <= 60
    # other emails are unaffected
    assert book(slot="s1", email="fine@example.com").status_code == 201
    # case and spaces do not dodge the limit
    assert book(slot="s2", email="  SPAM@example.com ").status_code == 429


def test_simultaneous_attempts_cannot_exceed_limit():
    with ThreadPoolExecutor(max_workers=25) as ex:
        codes = list(ex.map(lambda i: book(slot=f"m{i % 12}", email="burst@example.com").status_code, range(25)))
    assert codes.count(429) == 20, codes
    assert sum(1 for c in codes if c != 429) == 5


def test_idempotent_replays_do_not_burn_attempts():
    k = {"Idempotency-Key": str(uuid.uuid4())}
    for _ in range(10):
        assert book(email="again@example.com", headers=k).status_code == 201


def test_limit_recovers_after_window():
    for _ in range(5):
        book(slot="past", email="wait@example.com")
    r = book(email="wait@example.com")
    assert r.status_code == 429
    time.sleep(int(r.headers["Retry-After"]) + 1)
    assert book(email="wait@example.com").status_code == 201
