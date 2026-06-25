import asyncio
import aiohttp
from db import get_connection, setup_tables
from scrapers.genre_detail_scraper import (
    TokenBucket,
    sync_pending_genres,
    count_pending_genres,
    get_pending_genre_ids,
    fetch_batch,
    save_genre,
    BATCH_SIZE,
)
from config import TRACK_DETAIL_REQUESTS_PER_SECOND

PROGRESS_KEY = "genre"
CONCURRENCY  = 49


def run_genre_detail_scrape():
    asyncio.run(_run())


async def _run():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def handle_stop():
        print("\n[Genre] ⛔ Nhận tín hiệu dừng...")
        stop_event.set()

    import signal
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT,  handle_stop)

    try:
        inserted = sync_pending_genres(cur)
        conn.commit()

        cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
        row = cur.fetchone()
        last_id = row[0] if row else 0

        pending = count_pending_genres(cur)
        print(f"[Genre] pending: {pending} | synced mới: {inserted} | tiếp từ id > {last_id}")

        if pending == 0:
            print("[Genre] ✅ Không có gì cần cào, thoát.")
            return

        bucket = TokenBucket(TRACK_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        fetched = not_found = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                genre_ids = get_pending_genre_ids(cur, last_id, BATCH_SIZE)
                if not genre_ids:
                    completed_normally = True
                    break

                fetch_task = asyncio.create_task(
                    fetch_batch(session, genre_ids, bucket, sem)
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
                            for gid, genre in zip(genre_ids, results):
                                if genre:
                                    save_genre(cur, genre)
                                    fetched += 1
                                else:
                                    not_found += 1
                            last_id = genre_ids[-1]
                            cur.execute("""
                                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                                ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                            """, (PROGRESS_KEY, last_id))
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                for gid, genre in zip(genre_ids, results):
                    if genre:
                        save_genre(cur, genre)
                        fetched += 1
                        print(f"[Genre] [{gid}] {genre['name']}")
                    else:
                        not_found += 1
                        print(f"[Genre] [{gid}] not found")

                last_id = genre_ids[-1]
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, last_id))
                conn.commit()

                print(
                    f"[Genre] batch xong | "
                    f"fetched: {fetched} | not found: {not_found} | "
                    f"last_id: {last_id}"
                )

        if completed_normally:
            cur.execute("""
                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
                ON CONFLICT (scraper) DO UPDATE SET last_id = 0
            """, (PROGRESS_KEY,))
            conn.commit()
            print(f"[Genre] ✅ Hoàn thành toàn bộ | fetched: {fetched} | not found: {not_found}")
        else:
            print(f"[Genre] 💾 Dừng giữa chừng | fetched: {fetched} | not found: {not_found} | last_id giữ tại: {last_id}")

    except Exception as e:
        conn.rollback()
        print(f"[Genre] ⚠️  Lỗi: {e}")
        raise

    finally:
        cur.close()
        conn.close()