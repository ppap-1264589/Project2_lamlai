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
) -> dict:
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(
                ALBUM_URL.format(id=album_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 404:
                    return {"id": album_id, "scrape_status": "not_found"}
                if resp.status != 200:
                    return {"id": album_id, "scrape_status": "error"}
                
                data = await resp.json()
                if "error" in data:
                    return {"id": album_id, "scrape_status": "not_found"}
                
                # FIX 3: Tránh lỗi số 0 bị coi là False. Chỉ báo no_data khi tất cả đều None hoặc chuỗi rỗng
                fields = ["title", "genre_id", "release_date", "record_type", "fans"]
                has_data = any(data.get(f) not in (None, "") for f in fields)
                
                if not has_data:
                    return {"id": album_id, "scrape_status": "no_data"}
                
                return {
                    "id":            album_id,  # FIX 1: Ép buộc trả về đúng album_id yêu cầu ban đầu
                    "title":         data.get("title"),
                    "genre_id":      data.get("genre_id"),
                    "release_date":  data.get("release_date") or None,
                    "record_type":   data.get("record_type"),
                    "fans":          data.get("fans"),
                    "scrape_status": "done",
                }
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


# ── DB helpers ────────────────────────────────────────────────

def count_pending_albums(cur) -> int:
    cur.execute("""
        SELECT COUNT(*) FROM albums
        WHERE scrape_status = 'pending'
    """)
    return cur.fetchone()[0]


def get_pending_album_ids(cur, last_id: int, limit: int) -> list[int]:
    cur.execute("""
        SELECT id FROM albums
        WHERE scrape_status = 'pending' AND id > %s
        ORDER BY id
        LIMIT %s
    """, (last_id, limit))
    return [row[0] for row in cur.fetchall()]


def save_batch(cur, results: list[dict]):
    """Bulk upsert toàn bộ kết quả — cả done lẫn not_found/error đều được ghi status."""
    if not results:
        return

    # Sử dụng dict để deduplicate, CHÚ Ý ÉP KIỂU ID
    # Deduplicate theo id — async batch có thể trả về 2 result trùng id,
    # PostgreSQL không cho phép ON CONFLICT DO UPDATE cùng row 2 lần trong 1 statement.
    # Ưu tiên giữ 'done' hơn 'error'/'not_found' nếu trùng.
    seen: dict[int, dict] = {}
    
    for r in results:
        # Ép kiểu int để đồng nhất, triệt tiêu sự khác biệt giữa '123' và 123
        try:
            rid = int(r["id"])
        except (ValueError, TypeError):
            continue # Bỏ qua nếu id rác không thể ép về int
            
        status = r["scrape_status"]

        # Nếu id chưa có trong seen, HOẶC nếu status mới là 'done' thì ghi đè
        if rid not in seen or status == "done":
            seen[rid] = r

    # Lúc này mảng rows đảm bảo 100% không có ID trùng lặp
    rows = [
        (
            int(r["id"]),  # Ép kiểu ở đây luôn cho chắc chắn
            r.get("title"),
            r.get("genre_id"),
            r.get("release_date"),
            r.get("record_type"),
            r.get("fans"),
            r["scrape_status"],
        )
        for r in seen.values()
    ]

    # Bỏ qua phần deduplicate vì mình đã hướng dẫn bạn ở câu trước
    # Chỉ thay đổi phần câu lệnh execute_values:
    
    execute_values(cur, """
        INSERT INTO albums (id, title, genre_id, release_date, record_type, fans,
                            scrape_status, scrape_attempted_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            -- FIX 2: Ưu tiên dữ liệu mới cào (EXCLUDED) đè lên dữ liệu cũ (albums)
            title               = COALESCE(EXCLUDED.title,        albums.title),
            genre_id            = COALESCE(EXCLUDED.genre_id,     albums.genre_id),
            release_date        = COALESCE(EXCLUDED.release_date, albums.release_date),
            record_type         = COALESCE(EXCLUDED.record_type,  albums.record_type),
            fans                = COALESCE(EXCLUDED.fans,         albums.fans),
            scrape_status       = EXCLUDED.scrape_status,
            scrape_attempted_at = EXCLUDED.scrape_attempted_at
    """, rows, template="(%s, %s, %s, %s, %s, %s, %s, NOW())")