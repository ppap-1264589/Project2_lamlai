"""
Debug script: xem raw response từ Deezer album detail API.

Chạy: docker compose run --rm --no-deps scraper python debug/debug_album_detail.py

Không cần DB. Chỉ cần container 'scraper' (và internet/VPN).
"""

import asyncio
import time
import aiohttp
import json
import sys
import os

# Cho phép import config.py từ thư mục gốc khi chạy file này trong debug/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import ALBUM_URL, HEADERS

# ── Cấu hình ─────────────────────────────────────────────────
CONCURRENCY   = 49        # Giữ nguyên như production
RATE_PER_SEC  = 9.5      # Giữ nguyên như production
BATCH_SIZE    = 49        # 1 batch = 49 album
NUM_BATCHES   = 3         # Cào bao nhiêu batch? (mặc định 3 = 147 albums)

# Thay đổi START_ALBUM_ID nếu muốn test từ một vùng ID cụ thể.
# Đặt None để tự động lấy ngẫu nhiên một đoạn ID có thật.
START_ALBUM_ID: int | None = 63003390

# ── Token Bucket (copy từ production) ────────────────────────
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


# ── Fetch với verbose logging ─────────────────────────────────
async def fetch_one(
    session: aiohttp.ClientSession,
    album_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
    results: list,
):
    await bucket.acquire()
    async with sem:
        url = ALBUM_URL.format(id=album_id)
        t0  = time.perf_counter()
        try:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                elapsed = time.perf_counter() - t0
                status  = resp.status

                try:
                    raw_text = await resp.text()
                except Exception as e:
                    raw_text = f"[CANNOT READ BODY: {e}]"

                try:
                    data = json.loads(raw_text)
                except Exception:
                    data = None

                entry = {
                    "album_id": album_id,
                    "http_status": status,
                    "elapsed_ms": round(elapsed * 1000),
                    "raw_text_preview": raw_text[:300],  # 300 ký tự đầu
                    "has_error_key": "error" in (data or {}),
                    "error_detail": data.get("error") if data else None,
                    "parsed_ok": data is not None,
                }

                # Classify như production code
                if status == 404:
                    entry["verdict"] = "not_found (404)"
                elif status != 200:
                    entry["verdict"] = f"ERROR (HTTP {status})"
                elif data and "error" in data:
                    entry["verdict"] = f"not_found (error in body: {data['error']})"
                elif data:
                    fields = ["title", "genre_id", "release_date", "record_type", "fans"]
                    has_data = any(data.get(f) not in (None, "") for f in fields)
                    entry["verdict"] = "done" if has_data else "not_necess"
                else:
                    entry["verdict"] = "ERROR (unparseable JSON)"

                results.append(entry)

        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - t0
            results.append({
                "album_id": album_id,
                "http_status": None,
                "elapsed_ms": round(elapsed * 1000),
                "verdict": "ERROR (timeout)",
                "error_detail": "asyncio.TimeoutError after 10s",
            })
        except Exception as e:
            elapsed = time.perf_counter() - t0
            results.append({
                "album_id": album_id,
                "http_status": None,
                "elapsed_ms": round(elapsed * 1000),
                "verdict": f"ERROR (exception)",
                "error_detail": repr(e),
            })


# ── Main ──────────────────────────────────────────────────────
async def main():
    start_id = START_ALBUM_ID if START_ALBUM_ID is not None else 1

    bucket = TokenBucket(RATE_PER_SEC)
    sem    = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(family=2)  # AF_INET = IPv4 only (giống Dockerfile)
    async with aiohttp.ClientSession(connector=connector) as session:

        for batch_num in range(1, NUM_BATCHES + 1):
            album_ids = list(range(start_id, start_id + BATCH_SIZE))
            results: list[dict] = []

            print(f"\n{'='*60}")
            print(f"  BATCH {batch_num}: album_id {album_ids[0]} → {album_ids[-1]}")
            print(f"{'='*60}")

            t_batch = time.perf_counter()
            tasks   = [
                fetch_one(session, aid, bucket, sem, results)
                for aid in album_ids
            ]
            await asyncio.gather(*tasks)
            batch_elapsed = time.perf_counter() - t_batch

            # Sắp xếp lại theo album_id cho dễ đọc
            results.sort(key=lambda r: r["album_id"])

            # ── In từng dòng ──────────────────────────────────
            errors = []
            for r in results:
                verdict = r["verdict"]
                ms      = r.get("elapsed_ms", "?")
                tag     = "✓" if verdict == "done" else ("~" if "not_found" in verdict or "not_necess" in verdict else "✗")
                print(f"  {tag} [{r['album_id']:>10}] {ms:>5}ms  {verdict}")

                if tag == "✗":
                    errors.append(r)

            # ── Tóm tắt batch ─────────────────────────────────
            counts = {}
            for r in results:
                v = r["verdict"]
                counts[v] = counts.get(v, 0) + 1

            print(f"\n  >> Batch thời gian: {batch_elapsed:.2f}s")
            print(f"  >> Tổng: {len(results)} | Phân loại: {counts}")

            # ── Chi tiết lỗi ──────────────────────────────────
            if errors:
                print(f"\n  !! {len(errors)} LỖI CHI TIẾT:")
                for e in errors:
                    print(f"\n     album_id    : {e['album_id']}")
                    print(f"     http_status : {e.get('http_status')}")
                    print(f"     elapsed_ms  : {e.get('elapsed_ms')}ms")
                    print(f"     error_detail: {e.get('error_detail')}")
                    preview = e.get("raw_text_preview", "")
                    if preview:
                        print(f"     raw_preview : {preview}")

            start_id += BATCH_SIZE

    print("\n\nXong! Nếu thấy nhiều timeout → tăng ClientTimeout hoặc giảm CONCURRENCY.")
    print("Nếu thấy HTTP 429 → rate limit bị vượt, giảm RATE_PER_SEC.")
    print("Nếu thấy lỗi JSON → có thể Deezer trả về HTML (bị block).\n")


if __name__ == "__main__":
    asyncio.run(main())