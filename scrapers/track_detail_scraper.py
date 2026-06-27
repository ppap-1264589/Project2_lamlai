import asyncio
import time
import aiohttp
from psycopg2.extras import execute_values
from config import TRACK_URL, HEADERS

# Xử lý tình huống ngày không hợp lệ (năm = 0000)
def _parse_date(val):
    if not val or val.startswith("0000"):
        return None
    return val

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
                TRACK_URL.format(id=track_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": track_id, "scrape_status": "not_found"}
                if resp.status == 429:
                    return {"id": track_id, "scrape_status": "quota"}

                data = await resp.json(content_type=None)

                if "error" in data:
                    code = data["error"].get("code")
                    if code == 4:
                        return {"id": track_id, "scrape_status": "quota"}
                    # code 800 hoặc lỗi khác → not_found
                    return {"id": track_id, "scrape_status": "not_found"}

                fields = ["title", "duration", "rank", "bpm", "release_date", "available_countries"]
                has_data = any(data.get(f) not in (None, "", []) for f in fields)

                if not has_data:
                    return {"id": track_id, "scrape_status": "not_necess"}

                return {
                    "id":                  track_id,
                    "title":               data.get("title"),
                    "duration":            data.get("duration"),
                    "rank":                data.get("rank"),
                    "bpm":                 data.get("bpm"),
                    "release_date":        _parse_date(data.get("release_date")),
                    "available_countries": data.get("available_countries") or [],
                    "scrape_status":       "done",
                }

        except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError):
            return {"id": track_id, "scrape_status": "conn_error"}
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


    # Bulk insert track_countries — chỉ cho 'done'
    country_rows = [
        (r["id"], r.get("available_countries") or [])
        for r in seen.values()
        if r["scrape_status"] == "done" and r.get("available_countries")
    ]
    if country_rows:
        execute_values(cur, """
            INSERT INTO track_countries (track_id, countries)
            VALUES %s
            ON CONFLICT (track_id) DO NOTHING
        """, country_rows)