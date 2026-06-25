import asyncio
import time
import aiohttp
from psycopg2.extras import execute_values
from config import ALBUM_URL, TRACK_DETAIL_REQUESTS_PER_SECOND, HEADERS

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

async def fetch_album_detail(
    session: aiohttp.ClientSession,
    album_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict | None:
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                ALBUM_URL.format(id=album_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if "error" in data:
                    return None
                return {
                    "id":           int(data["id"]),
                    "title":        data.get("title"),
                    "genre_id":     data.get("genre_id"),
                    "release_date": data.get("release_date") or None,
                    "record_type":  data.get("record_type"),
                    "fans":         data.get("fans"),
                }
        except Exception:
            return None


async def fetch_batch(
    session: aiohttp.ClientSession,
    album_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict | None]:
    tasks = [fetch_album_detail(session, aid, bucket, sem) for aid in album_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def count_pending_albums(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM albums WHERE title IS NULL")
    return cur.fetchone()[0]


def get_pending_album_ids(cur, last_id: int, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM albums
        WHERE title IS NULL AND id > %s
        ORDER BY id
        LIMIT %s
    """, (last_id, limit))
    return [row[0] for row in cur.fetchall()]


def save_batch(cur, albums: list[dict]):
    if not albums:
        return
    execute_values(cur, """
        INSERT INTO albums (id, title, genre_id, release_date, record_type, fans)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title        = COALESCE(albums.title,        EXCLUDED.title),
            genre_id     = COALESCE(albums.genre_id,     EXCLUDED.genre_id),
            release_date = COALESCE(albums.release_date, EXCLUDED.release_date),
            record_type  = COALESCE(albums.record_type,  EXCLUDED.record_type),
            fans         = COALESCE(albums.fans,         EXCLUDED.fans)
    """, [
        (a["id"], a["title"], a["genre_id"], a["release_date"], a["record_type"], a["fans"])
        for a in albums
    ])