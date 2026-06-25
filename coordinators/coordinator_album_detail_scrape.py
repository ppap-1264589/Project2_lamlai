import asyncio
import aiohttp
from db import get_connection, setup_tables
from scrapers.album_detail_scraper import (
    TokenBucket,
    count_pending_albums,
    get_pending_album_ids,
    fetch_batch,
    save_batch,
    BATCH_SIZE,
)
from config import TRACK_DETAIL_REQUESTS_PER_SECOND

PROGRESS_KEY = "album_detail"
CONCURRENCY  = 49


def run_album_detail_scrape():
    asyncio.run(_run())


async def _run():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def handle_stop():
        print("\n[AlbumDetail] ⛔ Nhận tín hiệu dừng...", flush=True)
        stop_event.set()

    import signal
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT,  handle_stop)

    try:
        cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
        row = cur.fetchone()
        last_id = row[0] if row else 0

        pending = count_pending_albums(cur)
        print(f"[AlbumDetail] pending: {pending} | tiếp từ id > {last_id}", flush=True)

        if pending == 0:
            print("[AlbumDetail] ✅ Không có gì cần cào, thoát.", flush=True)
            return

        bucket = TokenBucket(TRACK_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        fetched = not_found = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                album_ids = get_pending_album_ids(cur, last_id, BATCH_SIZE)
                if not album_ids:
                    completed_normally = True
                    break

                fetch_task = asyncio.create_task(
                    fetch_batch(session, album_ids, bucket, sem)
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
                            albums = [a for a in results if a]
                            not_found += len(results) - len(albums)
                            fetched += len(albums)
                            if albums:
                                save_batch(cur, albums)
                            last_id = album_ids[-1]
                            cur.execute("""
                                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                                ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                            """, (PROGRESS_KEY, last_id))
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                albums = []
                for aid, album in zip(album_ids, results):
                    if album:
                        albums.append(album)
                        fetched += 1
                        print(f"[AlbumDetail] [{aid}] {album['title']}", flush=True)
                    else:
                        not_found += 1
                        print(f"[AlbumDetail] [{aid}] not found", flush=True)

                if albums:
                    save_batch(cur, albums)

                last_id = album_ids[-1]
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, last_id))
                conn.commit()

                print(
                    f"[AlbumDetail] batch xong | "
                    f"fetched: {fetched} | not found: {not_found} | "
                    f"last_id: {last_id}",
                    flush=True,
                )

        if completed_normally:
            cur.execute("""
                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
                ON CONFLICT (scraper) DO UPDATE SET last_id = 0
            """, (PROGRESS_KEY,))
            conn.commit()
            print(f"[AlbumDetail] ✅ Hoàn thành | fetched: {fetched} | not found: {not_found}", flush=True)
        else:
            print(f"[AlbumDetail] 💾 Dừng giữa chừng | fetched: {fetched} | not found: {not_found} | last_id: {last_id}", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"[AlbumDetail] ⚠️  Lỗi: {e}", flush=True)
        raise

    finally:
        cur.close()
        conn.close()