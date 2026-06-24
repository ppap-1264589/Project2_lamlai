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
MAX_ID         = 7_000_000_000
COARSE_SIZE    = 100_000_000     # Kích thước khối Phase 1
FINE_SIZE      =   1_000_000     # Kích thước khối Phase 2
# Tham số kiểm định tỉ lệ (Normal approximation)
# Chứng minh tỉ lệ ID hoạt động thực tế <= p, với độ tin cậy 99.9%
#   n = Z^2 * p * (1-p) / E^2
#   Z = 3.09  (alpha = 0.001, two-tailed 99.9%)
#   p = tỉ lệ hoạt động tối đa giả định (1%)
#   E = sai số biên cho phép (+-0.5%)
Z_SCORE         = 3.09
ASSUMED_DENSITY = 0.01    # p
MARGIN_ERROR    = 0.005   # E

SAMPLE_K = math.ceil((Z_SCORE**2 * ASSUMED_DENSITY * (1 - ASSUMED_DENSITY)) / MARGIN_ERROR**2)

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
COARSE_CSV     = os.path.join(SCRIPT_DIR, "coarse_scan.csv")
FINE_CSV       = os.path.join(SCRIPT_DIR, "fine_scan.csv")

CONCURRENCY    = 50
RATE_PER_SEC   = 9.0   # Deezer limit: 50 req/5s = 10/s, giữ 9 để có buffer

# ==============================================================================
# TOKEN BUCKET — rate limiter chính xác
# ==============================================================================

class TokenBucket:
    """Giới hạn throughput bằng token bucket. Thread-safe với asyncio."""
    def __init__(self, rate: float):
        self.rate        = rate
        self.tokens      = rate
        self.last_refill = time.perf_counter()
        self._lock       = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now     = time.perf_counter()
                elapsed = now - self.last_refill
                self.tokens      = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return          # có token → thoát ngay
                wait = (1 - self.tokens) / self.rate
            # Release lock TRƯỚC khi sleep → các coroutine khác vẫn vào được
            await asyncio.sleep(wait)

# ==============================================================================
# TIỆN ÍCH
# ==============================================================================

async def check_id(session: aiohttp.ClientSession, user_id: int, sem: asyncio.Semaphore, bucket: "TokenBucket") -> str:
    url = f"https://api.deezer.com/user/{user_id}"
    await bucket.acquire()          # đợi token trước khi gửi
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    return "Error" if "error" in data else "OK"
                return "Error"
        except Exception:
            return "Error"


BATCH_SIZE = 50   # số request gửi song song mỗi lượt

async def sample_block(
    session, sem, start: int, end: int, k: int,
    bucket: "TokenBucket",
    pbar: tqdm | None = None,
    postfix_extra: dict | None = None,
    early_exit: int = 4,
) -> tuple[int, int]:
    """
    Lấy tối đa k mẫu ngẫu nhiên trong [start, end], gửi theo batch BATCH_SIZE.
    Dừng sớm ngay khi ok_count >= early_exit.
    Trả về (ok_count, total_sent).
    """
    sample_ids = random.sample(range(start, end + 1), min(k, end - start + 1))

    done_count = 0
    ok_count   = 0
    t_start    = time.perf_counter()

    for batch_start in range(0, len(sample_ids), BATCH_SIZE):
        if ok_count >= early_exit:
            break

        batch = sample_ids[batch_start : batch_start + BATCH_SIZE]
    for batch_start in range(0, len(sample_ids), BATCH_SIZE):
        if ok_count >= early_exit:
            break

        batch = sample_ids[batch_start : batch_start + BATCH_SIZE]

        async def _one(sid: int) -> str:
            nonlocal done_count, ok_count
            result = await check_id(session, sid, sem, bucket)
            done_count += 1
            if result == "OK":
                ok_count += 1
            if pbar is not None:
                elapsed = time.perf_counter() - t_start
                rps = done_count / elapsed if elapsed > 0 else 0.0
                pbar.set_postfix({
                    **(postfix_extra or {}),
                    "req": f"{done_count}/{len(sample_ids)}",
                    "req/s": f"{rps:.1f}",
                })
            return result

        await asyncio.gather(*[_one(sid) for sid in batch], return_exceptions=True)

    return ok_count, done_count


def append_csv(filepath: str, row: dict):
    """Ghi 1 dòng vào CSV ngay lập tức (flush sau mỗi block)."""
    df = pd.DataFrame([row])
    write_header = not os.path.exists(filepath)
    df.to_csv(filepath, mode="a", header=write_header, index=False)


def load_done_indices(filepath: str, col: str) -> set:
    """Đọc các index đã xử lý từ CSV để resume."""
    if not os.path.exists(filepath):
        return set()
    try:
        df = pd.read_csv(filepath)
        return set(df[col].tolist())
    except Exception:
        return set()


# ==============================================================================
# PHASE 1: COARSE SCAN (khối 100M)
# ==============================================================================

async def phase1(session, sem, bucket: TokenBucket) -> list[int]:
    total_blocks = MAX_ID // COARSE_SIZE
    done = load_done_indices(COARSE_CSV, "block_index")

    print(f"\n{'='*60}")
    print(f"PHASE 1 — Coarse scan: {total_blocks} khối × {COARSE_SIZE:,} ID")
    print(f"Mẫu/khối: {SAMPLE_K} | Độ tin cậy: 99.9% | p={ASSUMED_DENSITY} | E=±{MARGIN_ERROR}")
    if done:
        print(f"Resume: đã có {len(done)}/{total_blocks} khối, bỏ qua.")
    print(f"{'='*60}")

    alive: list[int] = []

    # Nạp lại các khối sống từ CSV cũ
    if os.path.exists(COARSE_CSV):
        try:
            df = pd.read_csv(COARSE_CSV)
            alive = df[df["status"] == "ALIVE"]["block_index"].tolist()
        except Exception:
            pass

    pbar = tqdm(range(total_blocks), desc="Phase 1", unit="khối")
    for bi in pbar:
        if bi in done:
            pbar.set_postfix({"block": bi, "→": "skip"})
            continue

        start = bi * COARSE_SIZE + 1
        end   = min((bi + 1) * COARSE_SIZE, MAX_ID)

        ok, total = await sample_block(
            session, sem, start, end, SAMPLE_K,
            bucket=bucket,
            pbar=pbar,
            postfix_extra={"block": f"{bi}/{total_blocks-1}"},
            early_exit=4,
        )
        status = "ALIVE" if ok > 0 else "DEAD"

        row = {
            "block_index": bi,
            "range":       f"{start}-{end}",
            "sampled":     total,
            "ok_count":    ok,
            "status":      status,
        }
        append_csv(COARSE_CSV, row)   # ← ghi ngay sau mỗi khối

        if status == "ALIVE":
            alive.append(bi)

        # Tổng kết block — req/s không còn realtime ở đây, chỉ hiện status
        pbar.set_postfix({"block": f"{bi}/{total_blocks-1}", "ok": ok, "req": f"{total}/{SAMPLE_K}", "→": status})

    pbar.close()
    dead_count = total_blocks - len(alive)
    print(f"\n[Phase 1 xong] Sống: {len(alive)} | Chết: {dead_count} "
          f"({dead_count/total_blocks*100:.1f}% không gian đã loại)")
    return alive


# ==============================================================================
# PHASE 2: FINE SCAN (khối 1M, chỉ trong khối sống)
# ==============================================================================

async def phase2(session, sem, bucket: TokenBucket, alive_coarse: list[int]) -> list[dict]:
    fine_per_coarse = COARSE_SIZE // FINE_SIZE
    total_fine = len(alive_coarse) * fine_per_coarse
    done = load_done_indices(FINE_CSV, "block_index")

    print(f"\n{'='*60}")
    print(f"PHASE 2 — Fine scan: {len(alive_coarse)} khối sống × {fine_per_coarse} phân đoạn 1M")
    print(f"Tổng phân đoạn cần quét: {total_fine:,} | Mẫu/phân đoạn: {SAMPLE_K}")
    if done:
        print(f"Resume: đã có {len(done)} phân đoạn xong.")
    print(f"{'='*60}")

    alive_fine: list[dict] = []

    # Nạp lại khối 1M sống từ CSV cũ
    if os.path.exists(FINE_CSV):
        try:
            df = pd.read_csv(FINE_CSV)
            alive_fine = df[df["status"] == "ALIVE"].to_dict("records")
        except Exception:
            pass

    pbar = tqdm(total=total_fine, desc="Phase 2", unit="phân đoạn")
    for ci in alive_coarse:
        coarse_start = ci * COARSE_SIZE + 1
        for fi in range(fine_per_coarse):
            block_index = ci * fine_per_coarse + fi
            if block_index in done:
                pbar.update(1)
                continue

            start = coarse_start + fi * FINE_SIZE
            end   = min(start + FINE_SIZE - 1, MAX_ID)

            ok, total = await sample_block(
                session, sem, start, end, SAMPLE_K,
                bucket=bucket,
                pbar=pbar,
                postfix_extra={"seg": f"{block_index}"},
                early_exit=4,
            )
            status = "ALIVE" if ok > 0 else "DEAD"

            row = {
                "block_index":    block_index,
                "coarse_block":   ci,
                "range":          f"{start}-{end}",
                "sampled":        total,
                "ok_count":       ok,
                "status":         status,
            }
            append_csv(FINE_CSV, row)   # ← ghi ngay sau mỗi phân đoạn

            if status == "ALIVE":
                alive_fine.append(row)

            pbar.update(1)
            pbar.set_postfix({"seg": block_index, "ok": ok, "req": f"{total}/{SAMPLE_K}", "→": status})

    pbar.close()
    dead_count = total_fine - len(alive_fine)
    print(f"\n[Phase 2 xong] Phân đoạn 1M sống: {len(alive_fine)} | "
          f"Chết: {dead_count} ({dead_count/total_fine*100:.1f}% loại)")
    return alive_fine


# ==============================================================================
# MAIN
# ==============================================================================

async def main_async():
    print("=" * 60)
    print("DEEZER USER ID MAPPER  —  Two-Phase Hierarchical Scanner")
    print("=" * 60)
    print(f"Sample k = ceil(Z²×p×(1-p)/E²) = ceil({Z_SCORE}²×{ASSUMED_DENSITY}×{1-ASSUMED_DENSITY}/{MARGIN_ERROR}²) = {SAMPLE_K} mẫu/khối")
    print(f"Output: {SCRIPT_DIR}")

    sem    = asyncio.Semaphore(CONCURRENCY)
    bucket = TokenBucket(RATE_PER_SEC)

    try:
        async with aiohttp.ClientSession() as session:
            alive_coarse = await phase1(session, sem, bucket)
            if not alive_coarse:
                print("[!] Không có khối sống nào. Kết thúc.")
                return

            alive_fine = await phase2(session, sem, bucket, alive_coarse)

            print(f"\n{'='*60}")
            print(f"KẾT QUẢ CUỐI: {len(alive_fine)} phân đoạn 1M đã xác nhận sống")
            print(f"Sẵn sàng để bắt đầu cào trực tiếp.")
            print(f"Xem chi tiết: {FINE_CSV}")
            print(f"{'='*60}")

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[!] Người dùng dừng chương trình. Tiến trình đã lưu, có thể resume.")


if __name__ == "__main__":
    # Windows Proactor event loop ném ConnectionResetError vô nghĩa khi
    # server đóng connection (WinError 10054). Dùng SelectorEventLoop thay thế.
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n[!] Kết thúc.")