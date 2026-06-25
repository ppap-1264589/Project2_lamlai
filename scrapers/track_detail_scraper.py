import asyncio
import time
import aiohttp
from psycopg2.extras import execute_values
from config import TRACK_URL, TRACK_DETAIL_REQUESTS_PER_SECOND, HEADERS

BATCH_SIZE = 50  # số request gửi song song mỗi lượt


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
) -> dict | None:
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                TRACK_URL.format(id=track_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if "error" in data:
                    return None
                return {
                    "id":                  data["id"],
                    "bpm":                 data.get("bpm"),
                    "release_date":        data.get("release_date") or None,
                    "available_countries": data.get("available_countries") or [],
                }
        except Exception:
            return None


async def fetch_batch(
    session: aiohttp.ClientSession,
    track_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict | None]:
    tasks = [fetch_track_detail(session, tid, bucket, sem) for tid in track_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def count_pending_tracks(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NULL")
    return cur.fetchone()[0]


def get_pending_track_ids(cur, last_id: int, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM tracks
        WHERE bpm IS NULL AND id > %s
        ORDER BY id
        LIMIT %s
    """, (last_id, limit))
    return [row[0] for row in cur.fetchall()]


def save_batch(cur, details: list[dict]):
    """Bulk update tracks + insert available_countries cho cả batch."""
    if not details:
        return

    # UPDATE tracks — từng row vì giá trị khác nhau
    for detail in details:
        cur.execute("""
            UPDATE tracks
            SET bpm          = COALESCE(%s, bpm),
                release_date = COALESCE(%s, release_date)
            WHERE id = %s
        """, (detail["bpm"], detail["release_date"], detail["id"]))

    # Bulk insert available_countries
    country_rows = [
        (detail["id"], country)
        for detail in details
        for country in detail["available_countries"]
    ]
    if country_rows:
        execute_values(cur, """
            INSERT INTO track_available_countries (track_id, country)
            VALUES %s
            ON CONFLICT (track_id, country) DO NOTHING
        """, country_rows)