import signal
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

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\n[TrackDetail] ⛔ Nhận tín hiệu dừng...")
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

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

        fetched = not_found = 0

        async with aiohttp.ClientSession() as session:
            while running:
                # Lấy batch ID tiếp theo cần cào
                track_ids = get_pending_track_ids(cur, last_id, BATCH_SIZE)
                if not track_ids:
                    break

                # Fetch song song
                results = await fetch_batch(session, track_ids, bucket, sem)

                # Save tuần tự trên 1 connection
                details = []
                for tid, detail in zip(track_ids, results):
                    if detail:
                        details.append(detail)
                        fetched += 1
                    else:
                        not_found += 1

                if details:
                    save_batch(cur, details)

                # Cập nhật last_id theo batch (id lớn nhất trong batch)
                last_id = track_ids[-1]
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, last_id))
                conn.commit()

                total_done = fetched + not_found
                print(
                    f"[TrackDetail] batch xong | "
                    f"fetched: {fetched} | not found: {not_found} | "
                    f"last_id: {last_id}"
                )

        # Reset progress sau pass hoàn chỉnh
        cur.execute("""
            INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
            ON CONFLICT (scraper) DO UPDATE SET last_id = 0
        """, (PROGRESS_KEY,))
        conn.commit()
        print(f"[TrackDetail] ✅ Xong | fetched: {fetched} | not found: {not_found}")

    except Exception as e:
        conn.rollback()
        print(f"[TrackDetail] ⚠️  Lỗi: {e}")
        raise

    finally:
        cur.close()
        conn.close()