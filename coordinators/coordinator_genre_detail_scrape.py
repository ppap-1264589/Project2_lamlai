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
        last_id = -100 if row[0] == 0 else row[0]

        pending = count_pending_genres(cur)
        print(f"[Genre] pending: {pending} | synced mới: {inserted} | tiếp từ id > {last_id}")

        if pending == 0:
            print("[Genre] ✅ Không có gì cần cào, thoát.")
            return

        bucket = TokenBucket(TRACK_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        done_count = not_found = error_count = 0
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
                            for r in results:
                                save_genre(cur, r)
                            last_id = genre_ids[-1]
                            _save_progress(cur, PROGRESS_KEY, last_id)
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                for gid, r in zip(genre_ids, results):
                    if   r["scrape_status"] == "done":      done_count  += 1
                    elif r["scrape_status"] == "not_found": not_found   += 1
                    else:                                   error_count += 1
                    _log_genre(r)
                    save_genre(cur, r)

                last_id = genre_ids[-1]
                _save_progress(cur, PROGRESS_KEY, last_id)
                conn.commit()

                print(
                    f"[Genre] batch xong | "
                    f"done: {done_count} | not_found: {not_found} | error: {error_count} | "
                    f"last_id: {last_id}"
                )

        if completed_normally:
            cur.execute("""
                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
                ON CONFLICT (scraper) DO UPDATE SET last_id = 0
            """, (PROGRESS_KEY,))
            conn.commit()
            print(f"[Genre] ✅ Hoàn thành toàn bộ | done: {done_count} | not_found: {not_found} | error: {error_count}")
        else:
            print(f"[Genre] 💾 Dừng giữa chừng | done: {done_count} | not_found: {not_found} | error: {error_count} | last_id: {last_id}")

    except Exception as e:
        conn.rollback()
        print(f"[Genre] ⚠️  Lỗi: {e}")
        raise

    finally:
        cur.close()
        conn.close()


# ── helpers ───────────────────────────────────────────────────

def _log_genre(r: dict):
    status = r["scrape_status"]
    gid    = r["id"]
    if status == "done":
        print(f"[Genre] [{gid}] {r.get('name')}")
    else:
        print(f"[Genre] [{gid}] {status}")


def _save_progress(cur, key: str, last_id: int):
    cur.execute("""
        INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
        ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
    """, (key, last_id))