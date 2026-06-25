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
) -> dict | None:
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                GENRE_URL.format(id=genre_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if "error" in data or not data.get("name"):
                    return None
                return {"id": data.get("id") or genre_id, "name": data["name"]}
        except Exception:
            return None


async def fetch_batch(
    session: aiohttp.ClientSession,
    genre_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict | None]:
    tasks = [fetch_genre(session, gid, bucket, sem) for gid in genre_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def sync_pending_genres(cur) -> int:
    cur.execute("""
        INSERT INTO genres (id, name)
        SELECT DISTINCT genre_id, NULL
        FROM albums
        WHERE genre_id IS NOT NULL
        ON CONFLICT (id) DO NOTHING
    """)
    return cur.rowcount


def count_pending_genres(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM genres WHERE name IS NULL")
    return cur.fetchone()[0]


def get_pending_genre_ids(cur, last_id: int, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM genres
        WHERE name IS NULL AND id > %s
        ORDER BY id
        LIMIT %s
    """, (last_id, limit))
    return [row[0] for row in cur.fetchall()]


def save_genre(cur, genre: dict):
    cur.execute("""
        INSERT INTO genres (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, genres.name)
    """, (genre["id"], genre["name"]))