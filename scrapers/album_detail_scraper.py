import asyncio
import time
import aiohttp
from psycopg2.extras import execute_values
from config import ALBUM_URL, ALBUM_TRACKS, HEADERS

BATCH_SIZE = 100

# Xử lý tình huống ngày không hợp lệ (năm = 0000)
def _parse_date(val):
    if not val or val.startswith("0000"):
        return None
    return val


# ── Token Bucket rate limiter ─────────────────────────────────
# DONE LOGIC
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


# ── Async fetch album detail ──────────────────────────────────
# DONE LOGIC
async def fetch_album_detail(
    session: aiohttp.ClientSession,
    album_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict:
    """
    scrape_status:
      'done'        → API trả data hợp lệ
      'not_necess'  → API tồn tại nhưng Deezer không có data quan trọng
      'not_found'   → API xác nhận không tồn tại
      'quota'       → API xác nhận đã đạt giới hạn request (PHẢI retry sau)
      'conn_error'  → Lỗi thiết lập kết nối tức thời (PHẢI retry sau)
      'error'       → Các loại lỗi khác (CÓ THỂ TÙY CHỌN retry sau)
    """
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                ALBUM_URL.format(id=album_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": album_id, "scrape_status": "not_found"}
                if resp.status == 429:
                    return {"id": album_id, "scrape_status": "quota"}

                data = await resp.json(content_type=None)

                if "error" in data:
                    code = data["error"].get("code")
                    if code == 4:
                        return {"id": album_id, "scrape_status": "quota"}
                    # code 800 hoặc lỗi khác → not_found
                    return {"id": album_id, "scrape_status": "not_found"}

                fields = ["title", "genre_id", "duration", "release_date", "record_type", "fans"]
                has_data = any(data.get(f) not in (None, "") for f in fields)

                if not has_data:
                    return {"id": album_id, "scrape_status": "not_necess"}

                return {
                    "id":            album_id,
                    "title":         data.get("title"),
                    "genre_id":      data.get("genre_id"),
                    "duration":      data.get("duration"),
                    "release_date":  _parse_date(data.get("release_date")),
                    "record_type":   data.get("record_type"),
                    "fans":          data.get("fans"),
                    "scrape_status": "done",
                }

        except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError):
            return {"id": album_id, "scrape_status": "conn_error"}
        except Exception:
            return {"id": album_id, "scrape_status": "error"}


async def fetch_batch(
    session: aiohttp.ClientSession,
    album_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict]:
    tasks = [fetch_album_detail(session, aid, bucket, sem) for aid in album_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── Async fetch album tracks ──────────────────────────────────

async def fetch_album_tracks(
    session: aiohttp.ClientSession,
    album_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> dict:
    """Cào /album/{id}/tracks, trả về list track với track_position đầy đủ.
    Thực ra hàm này là cào từ album_id cố định ra, nên không cần check quá kĩ status
    """
    await bucket.acquire()
    async with sem:
        try:
            tracks = []
            url = ALBUM_TRACKS.format(id=album_id)
            while url:
                async with session.get(
                    url,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json(content_type=None)
                    if "error" in data or "data" not in data:
                        break
                    for item in data["data"]:
                        tracks.append({
                            "id":             item["id"],
                            "track_position": item.get("track_position"),
                        })
                    url = data.get("next")
            return {"album_id": album_id, "tracks": tracks}
        except Exception:
            return {"album_id": album_id, "tracks": []}


async def fetch_tracks_batch(
    session: aiohttp.ClientSession,
    album_ids: list[int],
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
) -> list[dict]:
    tasks = [fetch_album_tracks(session, aid, bucket, sem) for aid in album_ids]
    return await asyncio.gather(*tasks, return_exceptions=False)


# ── DB helpers ────────────────────────────────────────────────

def count_pending_albums(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) FROM albums
        WHERE scrape_status = 'pending'
    """)
    return cur.fetchone()[0]

# DONE LOGIC
def get_pending_album_ids(cur, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM albums
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

    rows = [
        (
            int(r["id"]),
            r.get("title"),
            r.get("genre_id"),
            r.get("duration"),
            r.get("release_date"),
            r.get("record_type"),
            r.get("fans"),
            r["scrape_status"],
        )
        for r in seen.values()
    ]

    execute_values(cur, """
        INSERT INTO albums (id, title, genre_id, duration, release_date, record_type, fans,
                            scrape_status, scrape_attempted_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title               = COALESCE(EXCLUDED.title,        albums.title),
            genre_id            = COALESCE(EXCLUDED.genre_id,     albums.genre_id),
            duration            = COALESCE(EXCLUDED.duration,     albums.duration),
            release_date        = COALESCE(EXCLUDED.release_date, albums.release_date),
            record_type         = COALESCE(EXCLUDED.record_type,  albums.record_type),
            fans                = COALESCE(EXCLUDED.fans,         albums.fans),
            scrape_status       = CASE
                                    WHEN albums.scrape_status = 'done' THEN 'done'
                                    ELSE EXCLUDED.scrape_status
                                  END,
            scrape_attempted_at = EXCLUDED.scrape_attempted_at
    """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, NOW())")


def save_album_tracks(cur, album_id: int, tracks: list[dict]):
    if not tracks:
        return

    # Seed track IDs
    execute_values(cur, """
        INSERT INTO tracks (id)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, [(t["id"],) for t in tracks])

    # Quan hệ album → track + track_position
    execute_values(cur, """
        INSERT INTO album_tracks (album_id, track_id, track_position)
        VALUES %s
        ON CONFLICT (album_id, track_id) DO UPDATE SET
            track_position = COALESCE(EXCLUDED.track_position, album_tracks.track_position)
    """, [(album_id, t["id"], t["track_position"]) for t in tracks])