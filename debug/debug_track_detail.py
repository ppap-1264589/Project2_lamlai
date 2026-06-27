"""
Debug script: xem raw response từ Deezer track detail API.

Chạy: xem hướng dẫn bên dưới
Không cần DB. Chỉ cần container 'scraper_artist' (và internet/VPN).
"""

import asyncio
import time
import aiohttp
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import TRACK_URL, HEADERS

# ── Cấu hình ─────────────────────────────────────────────────
CONCURRENCY   = 49
RATE_PER_SEC  = 9.5
BATCH_SIZE    = 49
NUM_BATCHES   = 3

START_TRACK_ID: int | None = None  # đặt số cụ thể nếu muốn test vùng ID nhất định


# ── Token Bucket ──────────────────────────────────────────────
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
    track_id: int,
    bucket: TokenBucket,
    sem: asyncio.Semaphore,
    results: list,
):
    await bucket.acquire()
    async with sem:
        url = TRACK_URL.format(id=track_id)
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
                    "track_id":         track_id,
                    "http_status":      status,
                    "elapsed_ms":       round(elapsed * 1000),
                    "raw_text_preview": raw_text[:300],
                    "has_error_key":    "error" in (data or {}),
                    "error_detail":     data.get("error") if data else None,
                    "parsed_ok":        data is not None,
                }

                if status == 404:
                    entry["verdict"] = "not_found (404)"
                elif status == 429:
                    entry["verdict"] = "quota (HTTP 429)"
                elif status != 200:
                    entry["verdict"] = f"ERROR (HTTP {status})"
                elif data and "error" in data:
                    code = data["error"].get("code")
                    if code == 4:
                        entry["verdict"] = "quota (code 4)"
                    else:
                        entry["verdict"] = f"not_found (error in body: {data['error']})"
                elif data:
                    has_data = bool(data.get("bpm")) or bool(data.get("release_date"))
                    entry["verdict"] = "done" if has_data else "not_necess"
                else:
                    entry["verdict"] = "ERROR (unparseable JSON)"

                results.append(entry)

        except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
            elapsed = time.perf_counter() - t0
            results.append({
                "track_id":    track_id,
                "http_status": None,
                "elapsed_ms":  round(elapsed * 1000),
                "verdict":     "conn_error",
                "error_detail": repr(e),
            })
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - t0
            results.append({
                "track_id":    track_id,
                "http_status": None,
                "elapsed_ms":  round(elapsed * 1000),
                "verdict":     "ERROR (timeout)",
                "error_detail": "asyncio.TimeoutError after 10s",
            })
        except Exception as e:
            elapsed = time.perf_counter() - t0
            results.append({
                "track_id":    track_id,
                "http_status": None,
                "elapsed_ms":  round(elapsed * 1000),
                "verdict":     "ERROR (exception)",
                "error_detail": repr(e),
            })


# ── Main ──────────────────────────────────────────────────────
async def main():
    start_id = START_TRACK_ID if START_TRACK_ID is not None else 1

    bucket = TokenBucket(RATE_PER_SEC)
    sem    = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(family=2)  # IPv4 only
    async with aiohttp.ClientSession(connector=connector) as session:

        for batch_num in range(1, NUM_BATCHES + 1):
            track_ids = list(range(start_id, start_id + BATCH_SIZE))
            results: list[dict] = []

            print(f"\n{'='*60}")
            print(f"  BATCH {batch_num}: track_id {track_ids[0]} → {track_ids[-1]}")
            print(f"{'='*60}")

            t_batch = time.perf_counter()
            tasks   = [fetch_one(session, tid, bucket, sem, results) for tid in track_ids]
            await asyncio.gather(*tasks)
            batch_elapsed = time.perf_counter() - t_batch

            results.sort(key=lambda r: r["track_id"])

            errors = []
            for r in results:
                verdict = r["verdict"]
                ms      = r.get("elapsed_ms", "?")
                if verdict == "done":
                    tag = "✓"
                elif verdict in ("not_necess",) or verdict.startswith("not_found"):
                    tag = "~"
                else:
                    tag = "✗"
                print(f"  {tag} [{r['track_id']:>12}] {ms:>5}ms  {verdict}")
                if tag == "✗":
                    errors.append(r)

            counts = {}
            for r in results:
                v = r["verdict"]
                counts[v] = counts.get(v, 0) + 1

            print(f"\n  >> Batch thời gian: {batch_elapsed:.2f}s")
            print(f"  >> Tổng: {len(results)} | Phân loại: {counts}")

            if errors:
                print(f"\n  !! {len(errors)} LỖI CHI TIẾT:")
                for e in errors:
                    print(f"\n     track_id    : {e['track_id']}")
                    print(f"     http_status : {e.get('http_status')}")
                    print(f"     elapsed_ms  : {e.get('elapsed_ms')}ms")
                    print(f"     error_detail: {e.get('error_detail')}")
                    preview = e.get("raw_text_preview", "")
                    if preview:
                        print(f"     raw_preview : {preview}")

            start_id += BATCH_SIZE

    print("\n\nXong!")
    print("conn_error  → ConnectionReset/ServerDisconnected, retry sau")
    print("quota       → Deezer rate limit, giảm RATE_PER_SEC")
    print("timeout     → Tăng ClientTimeout hoặc giảm CONCURRENCY\n")


if __name__ == "__main__":
    asyncio.run(main())