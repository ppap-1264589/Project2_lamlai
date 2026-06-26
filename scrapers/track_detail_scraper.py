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
        self.tokens      = rate
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
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": track_id, "scrape_status": "not_found"}
                if resp.status != 200:
                    return {"id": track_id, "scrape_status": "error"}
                data = await resp.json()

                if "error" in data:
                    return {"id": track_id, "scrape_status": "error"}
                if not data.get("bpm") and not data.get("release_date"):
                    return {"id": track_id, "scrape_status": "no_data"}
                return {
                    "id":                  track_id,
                    "bpm":                 data.get("bpm"),
                    "release_date":        data.get("release_date") or None,
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
    """Bulk update tracks + insert available_countries cho cả batch."""
    if not results:
        return

    # UPDATE tracks — ghi status cho tất cả, kể cả not_found/error
    for r in results:
        cur.execute("""
            UPDATE tracks
            SET bpm                 = COALESCE(%s, bpm),
                release_date        = COALESCE(%s, release_date),
                scrape_status       = %s,
                scrape_attempted_at = NOW()
            WHERE id = %s
        """, (r.get("bpm"), r.get("release_date"), r["scrape_status"], r["id"]))

    # Bulk insert available_countries — chỉ cho 'done'
    country_rows = [
        (r["id"], country)
        for r in results
        if r["scrape_status"] == "done"
        for country in r.get("available_countries", [])
    ]
    if country_rows:
        execute_values(cur, """
            INSERT INTO track_available_countries (track_id, country)
            VALUES %s
            ON CONFLICT (track_id, country) DO NOTHING
        """, country_rows)