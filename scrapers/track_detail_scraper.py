import asyncio
import time
import aiohttp
from psycopg2.extras import execute_values
from config import TRACK_URL, HEADERS

BATCH_SIZE = 49


# ── Token Bucket rate limiter ─────────────────────────────────

class TokenBucket:
    def __init__(self, rate: float):
        self.rate        = rate
        self.tokens      = 0
        self.last_refill = time.perf_counter()
        self._lock       = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now              = time.perf_counter()
                elapsed          = now - self.last_refill
                self.tokens      = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait)


# ── Async fetch ───────────────────────────────────────────────

async def fetch_track_detail(
    session: aiohttp.ClientSession,
    track_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Luôn trả về dict với ít nhất {"id", "scrape_status"}.
    scrape_status:
      'done'      → API trả data hợp lệ, có ít nhất bpm hoặc release_date
      'not_found' → API xác nhận ID không tồn tại
      'no_data'   → Track tồn tại nhưng Deezer không có bpm lẫn release_date
      'error'     → lỗi mạng / timeout
    """
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                TRACK_URL.format(id=track_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": track_id, "scrape_status": "not_found"}
                if resp.status == 429:
                    return {"id": track_id, "scrape_status": "rlimit"}
                data = await resp.json()

                if "error" in data:
                    return {"id": track_id, "scrape_status": "error"}
                if not data.get("bpm") and not data.get("release_date"):
                    return {"id": track_id, "scrape_status": "no_data"}
                return {
                    "id":                  track_id,
                    "title":               data.get("title"),
                    "duration":            data.get("duration"),
                    "rank":                data.get("rank"),
                    "bpm":                 data.get("bpm"),
                    "release_date":        data.get("release_date") if data.get("release_date") not in (None, "", "0000-00-00") else None,
                    "available_countries": data.get("available_countries") or [],
                    "scrape_status":       "done",
                }
        except Exception:
            return {"id": track_id, "scrape_status": "error"}


async def fetch_batch(
    session: aiohttp.ClientSession,
    track_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict]:
    tasks = [fetch_track_detail(session, tid, bucket, sem) for tid in track_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def count_pending_tracks(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) FROM tracks
        WHERE scrape_status = 'pending'
    """)
    return cur.fetchone()[0]


def get_pending_track_ids(cur, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM tracks
        WHERE scrape_status = 'pending'
        ORDER BY id
        LIMIT %s
    """, (limit,))
    return [row[0] for row in cur.fetchall()]


def save_batch(cur, results: list[dict]):
    if not results:
        return

    # Deduplicate — ưu tiên giữ 'done' nếu trùng
    seen: dict[int, dict] = {}
    for r in results:
        try:
            rid = int(r["id"])
        except (ValueError, TypeError):
            continue
        if rid not in seen or r["scrape_status"] == "done":
            seen[rid] = r

    # Upsert tracks
    for r in seen.values():
        cur.execute("""
            INSERT INTO tracks (id, title, duration, rank, bpm, release_date,
                                scrape_status, scrape_attempted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                title               = COALESCE(EXCLUDED.title,        tracks.title),
                duration            = COALESCE(EXCLUDED.duration,     tracks.duration),
                rank                = COALESCE(EXCLUDED.rank,         tracks.rank),
                bpm                 = COALESCE(EXCLUDED.bpm,          tracks.bpm),
                release_date        = COALESCE(EXCLUDED.release_date, tracks.release_date),
                scrape_status =     CASE
                                    WHEN tracks.scrape_status = 'done' THEN 'done'
                                    ELSE EXCLUDED.scrape_status
                                    END,
                scrape_attempted_at = EXCLUDED.scrape_attempted_at
        """, (r["id"], r.get("title"), r.get("duration"), r.get("rank"),
              r.get("bpm"), r.get("release_date"), r["scrape_status"]))

    # Bulk insert available_countries — chỉ cho 'done'
    country_rows = [
        (r["id"], country)
        for r in seen.values()
        if r["scrape_status"] == "done"
        for country in r.get("available_countries", [])
    ]
    if country_rows:
        execute_values(cur, """
            INSERT INTO track_available_countries (track_id, country)
            VALUES %s
            ON CONFLICT (track_id, country) DO NOTHING
        """, country_rows)