import asyncio
import time
import aiohttp
from config import GENRE_URL, HEADERS


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
"""
scrape_status:
    'done'        → API trả data hợp lệ
    'not_necess'  → API tồn tại nhưng Deezer không có data quan trọng
    'not_found'   → API xác nhận không tồn tại
    'quota'       → API xác nhận đã đạt giới hạn request (PHẢI retry sau)
    'conn_error'  → Lỗi thiết lập kết nối tức thời (PHẢI retry sau)
    'error'       → Các loại lỗi khác (CÓ THỂ TÙY CHỌN retry sau)
"""
async def fetch_genre(
    session: aiohttp.ClientSession,
    genre_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict:

    await bucket.acquire()

    async with sem:
        try:
            async with session.get(
                GENRE_URL.format(id=genre_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": genre_id, "scrape_status": "not_found"}
                if resp.status == 429:
                    return {"id": genre_id, "scrape_status": "quota"}

                data = await resp.json(content_type=None)
                # Chấp nhận tất cả các content-type, kể cả text/html
                # vì Deezer đôi khi trả về content-type không chuẩn
    
                if "error" in data:
                    code = data["error"].get("code")
                    if code == 4:
                        return {"id": genre_id, "scrape_status": "quota"}
                    # code 800 hoặc lỗi khác → not_found
                    return {"id": genre_id, "scrape_status": "not_found"}

                if not data.get("name"):
                    return {"id": genre_id, "scrape_status": "not_necess"}

                return {
                    "id":            genre_id,
                    "name":          data["name"],
                    "scrape_status": "done",
                }

        except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError):
            return {"id": genre_id, "scrape_status": "conn_error"}
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