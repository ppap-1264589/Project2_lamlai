import asyncio
import time
import aiohttp
from config import GENRE_URL, TRACK_DETAIL_REQUESTS_PER_SECOND, HEADERS

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

async def fetch_genre(
    session: aiohttp.ClientSession,
    genre_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict:
    """
    Luôn trả về dict với ít nhất {"id", "scrape_status"}.
    scrape_status:
      'done'      → API trả data hợp lệ
      'not_found' → API xác nhận không tồn tại
      'error'     → lỗi mạng / timeout → có thể retry sau
    """
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                GENRE_URL.format(id=genre_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": genre_id, "scrape_status": "not_found"}
                if resp.status != 200:
                    return {"id": genre_id, "scrape_status": "error"}
                data = await resp.json()      

                if "error" in data:
                    return {"id": genre_id, "scrape_status": "error"}
                if not data.get("name"):
                    return {"id": genre_id, "scrape_status": "no_data"}
                return {
                    "id":            genre_id,
                    "name":          data["name"],
                    "scrape_status": "done",
                }
        except Exception:
            return {"id": genre_id, "scrape_status": "error"}


async def fetch_batch(
    session: aiohttp.ClientSession,
    genre_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict]:
    tasks = [fetch_genre(session, gid, bucket, sem) for gid in genre_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def sync_pending_genres(cur) -> int:
    cur.execute("""
        INSERT INTO genres (id, name, scrape_status)
        SELECT DISTINCT genre_id, NULL, 'pending'
        FROM albums
        WHERE genre_id IS NOT NULL
        ON CONFLICT (id) DO NOTHING
    """)
    return cur.rowcount


def count_pending_genres(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) FROM genres
        WHERE scrape_status = 'pending'
    """)
    return cur.fetchone()[0]


def get_pending_genre_ids(cur, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM genres
        WHERE scrape_status = 'pending'
        ORDER BY id
        LIMIT %s
    """, (limit,))
    return [row[0] for row in cur.fetchall()]


def save_genre(cur, result: dict):
    cur.execute("""
        INSERT INTO genres (id, name, scrape_status, scrape_attempted_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name                = COALESCE(EXCLUDED.name, genres.name),
            scrape_status       = EXCLUDED.scrape_status,
            scrape_attempted_at = NOW()
    """, (result["id"], result.get("name"), result["scrape_status"]))