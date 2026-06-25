import asyncio
import aiohttp
from db import get_connection, setup_tables
from scrapers.track_detail_scraper import (
    TokenBucket,
    count_pending_tracks,
    get_pending_track_ids,
    fetch_batch,
    save_batch,
    BATCH_SIZE,
)
from config import TRACK_DETAIL_REQUESTS_PER_SECOND

PROGRESS_KEY = "track_detail"
CONCURRENCY  = 49


def run_track_detail_scrape():
    asyncio.run(_run())


async def _run():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_stop():
        print("\n[TrackDetail] ⛔ Nhận tín hiệu dừng...")
        stop_event.set()

    import signal
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT,  handle_stop)

    try:
        cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
        row = cur.fetchone()
        last_id = row[0] if row else 0

        pending = count_pending_tracks(cur)
        print(f"[TrackDetail] pending: {pending} | tiếp từ id > {last_id}")

        if pending == 0:
            print("[TrackDetail] ✅ Không có gì cần cào, thoát.")
            return

        bucket = TokenBucket(TRACK_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        done_count = not_found = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                track_ids = get_pending_track_ids(cur, last_id, BATCH_SIZE)
                if not track_ids:
                    completed_normally = True
                    break

                fetch_task = asyncio.create_task(
                    fetch_batch(session, track_ids, bucket, sem)
                )
                stop_task = asyncio.create_task(stop_event.wait())

                done, pending_tasks = await asyncio.wait(
                    [fetch_task, stop_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for t in pending_tasks:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

                if stop_event.is_set():
                    if fetch_task in done and not fetch_task.cancelled():
                        try:
                            results = fetch_task.result()
                            # SỬA DÒNG NÀY:
                            done_count, not_found = _tally(results, done_count, not_found)
                            save_batch(cur, results)
                            last_id = track_ids[-1]
                            _save_progress(cur, PROGRESS_KEY, last_id)
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                done_count, not_found = _tally(results, done_count, not_found) 
                save_batch(cur, results)

                last_id = track_ids[-1]
                _save_progress(cur, PROGRESS_KEY, last_id)
                conn.commit()

                print(
                    f"[TrackDetail] batch xong | "
                    f"done: {done_count} | not_found: {not_found} | "
                    f"last_id: {last_id}"
                )

        if completed_normally:
            cur.execute("""
                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
                ON CONFLICT (scraper) DO UPDATE SET last_id = 0
            """, (PROGRESS_KEY,))
            conn.commit()
            print(f"[TrackDetail] ✅ Hoàn thành toàn bộ | done: {done_count} | not_found: {not_found}")
        else:
            print(f"[TrackDetail] 💾 Dừng giữa chừng | done: {done_count} | not_found: {not_found} | last_id giữ tại: {last_id}")

    except Exception as e:
        conn.rollback()
        print(f"[TrackDetail] ⚠️  Lỗi: {e}")
        raise

    finally:
        cur.close()
        conn.close()


# ── helpers ───────────────────────────────────────────────────

def _tally(results: list[dict], done_count: int, not_found: int) -> tuple[int, int]:
    for r in results:
        if r["scrape_status"] == "done":
            done_count += 1
        else:
            not_found  += 1
            
    # Bắt buộc phải return lại 2 giá trị này
    return done_count, not_found


def _save_progress(cur, key: str, last_id: int):
    cur.execute("""
        INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
        ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
    """, (key, last_id))