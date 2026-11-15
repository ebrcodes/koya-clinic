# Clinic booking API

A small booking service for a clinic: a JSON API plus a single page web app served from the same address.
Built with Python, FastAPI and Postgres.

## How the hard parts work

| Level | What | How |
|---|---|---|
| 1 | List slots, book, look up | Plain endpoints. `POST /reset` replaces all slots and clears bookings. |
| 2 | API key and validation | `X-API-Key` is checked on every API request (401 if missing or wrong, compared in constant time). Name and email are trimmed, email is lower cased and checked, and the slot must exist and not have started. |
| 3 | No double booking, cancel | A partial unique index on `bookings(slot_id) WHERE status = 'confirmed'`. The database refuses a second confirmed booking, so it holds even for simultaneous requests and across several server processes. Cancelling sets the status to `cancelled`, which frees the slot. |
| 4 | Safe retries, paging | `Idempotency-Key` responses are stored. A retry returns the stored response, the same key with a different body returns 422, and a per key lock makes simultaneous retries safe. Bookings are listed oldest first with an opaque cursor. |
| 5 | Spam limit | At most 5 attempts per email per minute, failed attempts included. The 6th gets 429 with `Retry-After` in whole seconds. The check and the count run under a per email lock, so bursts cannot slip through. Replays of an idempotent request do not use up attempts. |

All state is in Postgres, so nothing is lost on a restart.

## Choices where the spec was silent

* `POST /reset` answers `200 {"slots": <count>}`. It also clears stored idempotency keys and rate limit counters so every test run starts clean.
* Validation runs in this order: rate limit, missing or invalid fields (400), unknown slot (404), slot already started (400), slot taken (409).
* `GET /bookings` without `email`, with a bad `limit`, or with a bad `cursor` returns 400. Cancelled bookings are included in the list.
* Idempotent replays store every answer except 429, because a rate limit answer should be judged again on retry.
* The web page is given the API key by the server when it loads. That is fine for this exercise, but a real product would put a login or a server side proxy in front instead.
* The page uses the Bricolage Grotesque font, stored in `app/static/fonts` (SIL Open Font License), so it loads nothing from outside services. Only the `/static/` font files and the page itself are reachable without the API key.
* On first start, if there are no slots, a few demo slots are created so the page is usable. Set `SEED_DEMO=false` to turn that off.

## Run it locally

    pip install -r requirements.txt
    export API_KEY=choose-a-long-random-string
    export DATABASE_URL=postgresql://user:password@localhost:5432/clinic
    uvicorn app.main:app --port 8000 --workers 2

Open http://localhost:8000 for the web page.

## Tests

With the server running:

    pip install pytest httpx
    BASE_URL=http://localhost:8000 API_KEY=choose-a-long-random-string pytest -q tests

The suite covers every level, including 30 simultaneous bookings for one slot and 20 simultaneous retries with one key.
One test waits for the rate limit window to pass, so the full run takes about a minute.

## Deploy (Vercel and Neon, both free)

1. Neon: create a project in Frankfurt. Copy the connection string and use the **pooled** one (the host contains `-pooler`). It ends in `sslmode=require`.
2. Put this folder in a GitHub repository and import it in Vercel (Add New, Project). Vercel finds `app/main.py` on its own.
3. Before the first deploy, add these environment variables:
   * `API_KEY`: any long random string. Give the same value to Koya.
   * `DATABASE_URL`: the Neon pooled string.
   * `POOL_MAX`: `3` (each running copy of the app keeps a small pool).
4. Deploy. Open `https://<your-project>.vercel.app/health`. It should show `{"status":"ok"}`.

`vercel.json` pins the function to Frankfurt so it sits next to the database. Vercel does not put the app to sleep. After a quiet spell Neon's database wakes on the next request, which can add a second or two once.
