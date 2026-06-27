import os
import math
import random
import asyncio
import time
import aiohttp
import pandas as pd
from tqdm import tqdm

# ==============================================================================
# CẤU HÌNH
# ==============================================================================
MAX_ID          = 7_000_000_000
BLOCK_SIZE      = 100_000_000     # Mỗi block 100M ID

# Cỡ mẫu theo Binomial trực tiếp:
#   Câu hỏi: nếu thử k lần không thấy gì, có thể kết luận DEAD với tin cậy 1-α?
#   P(không thấy gì | p_thực ≥ p0) = (1 - p0)^k ≤ α
#   → k ≥ ln(α) / ln(1 - p0)
#
#   p0    = 0.01  → nếu block có ít nhất 1% ID sống, ta sẽ phát hiện ra
#   alpha = 0.001 → xác suất bỏ sót block sống chỉ 0.1%
#
#   Kết quả: k = 688 (so với ~3800 của Normal approx — giảm 5.5×)
ASSUMED_DENSITY = 0.01    # p0: ngưỡng mật độ tối thiểu để coi là ALIVE
ALPHA           = 0.001   # xác suất kết luận sai DEAD khi thực ra ALIVE
SAMPLE_K        = math.ceil(math.log(ALPHA) / math.log(1 - ASSUMED_DENSITY))

CONCURRENCY     = 50
RATE_PER_SEC    = 9.0    # Deezer: 50 req/5s → giữ 9/s để có buffer

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV  = os.path.join(SCRIPT_DIR, "coarse_scan.csv")
BATCH_SIZE  = 50    # số request gửi song song mỗi lượt

# ==============================================================================
# TOKEN BUCKET — rate limiter
# ==============================================================================

class TokenBucket:
    def __init__(self, rate: float):
        self.rate        = rate
        self.tokens      = 0
        self.last_refill = time.perf_counter()
        self._lock       = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now          = time.perf_counter()
                elapsed      = now - self.last_refill
                self.tokens  = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait)

# ==============================================================================
# HTTP CHECK
# ==============================================================================

async def check_id(
    session: aiohttp.ClientSession,
    user_id: int,
    sem: asyncio.Semaphore,
    bucket: TokenBucket,
) -> bool:
    """Trả về True nếu user_id tồn tại và hợp lệ."""
    url = f"https://api.deezer.com/user/{user_id}"
    await bucket.acquire()
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10)) as r:
                if r.status != 200:
                    return False
                data = await r.json()
                return "error" not in data
        except Exception:
            return False

# ==============================================================================
# SAMPLE BLOCK
# ==============================================================================

async def sample_block(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    bucket: TokenBucket,
    start: int,
    end: int,
    k: int,
    pbar: tqdm,
    postfix_extra: dict,
) -> tuple[int, int]:
    """
    Lấy k mẫu ngẫu nhiên trong [start, end], gửi theo batch BATCH_SIZE.
    Dừng sớm khi ok_count >= EARLY_EXIT.
    Trả về (ok_count, total_sent).
    """
    population  = min(k, end - start + 1)
    sample_ids  = random.sample(range(start, end + 1), population)

    ok_count    = 0
    done_count  = 0
    t_start     = time.perf_counter()

    for batch_start in range(0, len(sample_ids), BATCH_SIZE):
        if ok_count > 0:
            break  # tìm thấy 1 là đủ → dừng ngay, block này ALIVE

        batch = sample_ids[batch_start : batch_start + BATCH_SIZE]

        async def _one(sid: int) -> None:
            nonlocal ok_count, done_count
            alive = await check_id(session, sid, sem, bucket)
            done_count += 1
            if alive:
                ok_count += 1
            elapsed = time.perf_counter() - t_start
            rps = done_count / elapsed if elapsed > 0 else 0.0
            pbar.set_postfix({
                **postfix_extra,
                "req": f"{done_count}/{population}",
                "req/s": f"{rps:.1f}",
            })

        await asyncio.gather(*[_one(sid) for sid in batch], return_exceptions=True)

    return ok_count, done_count

# ==============================================================================
# CSV HELPERS
# ==============================================================================

def append_row(filepath: str, row: dict) -> None:
    df = pd.DataFrame([row])
    write_header = not os.path.exists(filepath)
    df.to_csv(filepath, mode="a", header=write_header, index=False)


def load_done_blocks(filepath: str) -> set[int]:
    if not os.path.exists(filepath):
        return set()
    try:
        return set(pd.read_csv(filepath)["block_index"].tolist())
    except Exception:
        return set()

# ==============================================================================
# MAIN SCAN
# ==============================================================================

async def run_scan(session: aiohttp.ClientSession, sem: asyncio.Semaphore, bucket: TokenBucket) -> None:
    total_blocks = math.ceil(MAX_ID / BLOCK_SIZE)
    done_blocks  = load_done_blocks(OUTPUT_CSV)

    print(f"\n{'='*60}")
    print(f"DEEZER COARSE SCAN — {total_blocks} blocks × {BLOCK_SIZE:,} IDs")
    print(f"MAX_ID: {MAX_ID:,} | Sample k={SAMPLE_K}")
    print(f"p0={ASSUMED_DENSITY} | α={ALPHA} → P(miss ALIVE block) ≤ {ALPHA*100}%")
    print(f"Logic: ALIVE nếu tìm thấy ≥1 hit | DEAD nếu {SAMPLE_K} mẫu đều trống")
    if done_blocks:
        print(f"Resume: {len(done_blocks)}/{total_blocks} blocks already done.")
    print(f"Output: {OUTPUT_CSV}")
    print(f"{'='*60}\n")

    alive_count = 0
    pbar = tqdm(range(total_blocks), desc="Scanning", unit="block")

    for bi in pbar:
        if bi in done_blocks:
            pbar.set_postfix({"block": bi, "→": "skip"})
            continue

        start = bi * BLOCK_SIZE + 1
        end   = min((bi + 1) * BLOCK_SIZE, MAX_ID)

        ok, sent = await sample_block(
            session, sem, bucket,
            start, end, SAMPLE_K,
            pbar=pbar,
            postfix_extra={"block": f"{bi}/{total_blocks-1}"},
        )

        status = "ALIVE" if ok > 0 else "DEAD"
        if status == "ALIVE":
            alive_count += 1

        append_row(OUTPUT_CSV, {
            "block_index": bi,
            "range_start": start,
            "range_end":   end,
            "sampled":     sent,
            "ok_count":    ok,
            "status":      status,
        })

        pbar.set_postfix({"block": bi, "ok": ok, "sent": sent, "→": status})

    pbar.close()

    dead_count = total_blocks - alive_count
    print(f"\n{'='*60}")
    print(f"DONE — ALIVE: {alive_count} | DEAD: {dead_count} "
          f"({dead_count/total_blocks*100:.1f}% eliminated)")
    print(f"Report saved to: {OUTPUT_CSV}")
    print(f"{'='*60}")


async def main():
    sem    = asyncio.Semaphore(CONCURRENCY)
    bucket = TokenBucket(RATE_PER_SEC)

    try:
        async with aiohttp.ClientSession() as session:
            await run_scan(session, sem, bucket)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[!] Interrupted — progress saved, safe to resume.")


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Done.")