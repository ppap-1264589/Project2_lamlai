import asyncio
import aiohttp
from db import get_connection, setup_tables
from scrapers.album_detail_scraper import (
    TokenBucket,
    count_pending_albums,
    get_pending_album_ids,
    fetch_batch,
    fetch_tracks_batch,
    save_batch,
    save_album_tracks,
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

        done_count = not_found = error_count = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                album_ids = get_pending_album_ids(cur, last_id, BATCH_SIZE)
                if not album_ids:
                    completed_normally = True
                    break

                # ── Bước 1: fetch album detail ────────────────
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
                            _tally(results, done_count, not_found, error_count)
                            save_batch(cur, results)
                            # Vẫn lưu tracks cho batch cuối trước khi dừng
                            done_ids = [int(r["id"]) for r in results if r["scrape_status"] == "done"]
                            if done_ids:
                                track_results = await fetch_tracks_batch(session, done_ids, bucket, sem)
                                for tr in track_results:
                                    save_album_tracks(cur, tr["album_id"], tr["tracks"])
                            last_id = album_ids[-1]
                            _save_progress(cur, PROGRESS_KEY, last_id)
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                for r in results:
                    if   r["scrape_status"] == "done":      done_count  += 1
                    elif r["scrape_status"] == "not_found": not_found   += 1
                    else:                                   error_count += 1
                    _log_album(r)

                # ── Bước 2: save album detail ─────────────────
                save_batch(cur, results)

                # ── Bước 3: fetch + save tracks cho album done ─
                done_ids = [int(r["id"]) for r in results if r["scrape_status"] == "done"]
                if done_ids:
                    print(f"[AlbumDetail] fetch tracks cho {len(done_ids)} album...", flush=True)
                    track_results = await fetch_tracks_batch(session, done_ids, bucket, sem)
                    for tr in track_results:
                        save_album_tracks(cur, tr["album_id"], tr["tracks"])

                last_id = album_ids[-1]
                _save_progress(cur, PROGRESS_KEY, last_id)
                conn.commit()

                print(
                    f"[AlbumDetail] batch xong | "
                    f"done: {done_count} | not_found: {not_found} | error: {error_count} | "
                    f"last_id: {last_id}",
                    flush=True,
                )

        if completed_normally:
            cur.execute("""
                INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
                ON CONFLICT (scraper) DO UPDATE SET last_id = 0
            """, (PROGRESS_KEY,))
            conn.commit()
            print(
                f"[AlbumDetail] ✅ Hoàn thành | "
                f"done: {done_count} | not_found: {not_found} | error: {error_count}",
                flush=True,
            )
        else:
            print(
                f"[AlbumDetail] 💾 Dừng giữa chừng | "
                f"done: {done_count} | not_found: {not_found} | error: {error_count} | "
                f"last_id: {last_id}",
                flush=True,
            )

    except Exception as e:
        conn.rollback()
        print(f"[AlbumDetail] ⚠️  Lỗi: {e}", flush=True)
        raise

    finally:
        cur.close()
        conn.close()


# ── helpers ───────────────────────────────────────────────────

def _tally(results, done_count, not_found, error_count):
    for r in results:
        if   r["scrape_status"] == "done":      done_count  += 1
        elif r["scrape_status"] == "not_found": not_found   += 1
        else:                                   error_count += 1


def _log_album(r: dict):
    status = r["scrape_status"]
    aid    = r["id"]
    if status == "done":
        print(f"[AlbumDetail] [{aid}] {r.get('title')}", flush=True)
    else:
        print(f"[AlbumDetail] [{aid}] {status}", flush=True)


def _save_progress(cur, key: str, last_id: int):
    cur.execute("""
        INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
        ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
    """, (key, last_id))