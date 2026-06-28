import asyncio
import aiohttp
from db import get_connection, setup_tables
from scrapers.track_detail_scraper import (
    TokenBucket,
    count_pending_tracks,
    get_pending_track_ids,
    fetch_batch,
    save_batch,
)
from config import TRACK_DETAIL_REQUESTS_PER_SECOND, CONCURRENCY, BATCH_SIZE

def run_track_detail_scrape():
    asyncio.run(_run())


async def _run():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_stop():
        print("\n[TrackDetail] ⛔ Nhận tín hiệu dừng...", flush=True)
        stop_event.set()

    import signal
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT,  handle_stop)

    try:
        pending = count_pending_tracks(cur)
        print(f"[TrackDetail] pending: {pending}", flush=True)

        if pending == 0:
            print("[TrackDetail] ✅ Không có gì cần cào, thoát.", flush=True)
            return

        bucket = TokenBucket(TRACK_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        done_count = not_found = not_necess = quota = conn_error = error_count = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                track_ids = get_pending_track_ids(cur, BATCH_SIZE)
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
                            for r in results:
                                done_count, not_found, not_necess, quota, conn_error, error_count = _tally(
                                    r, done_count, not_found, not_necess, quota, conn_error, error_count
                                )
                            save_batch(cur, results)
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                save_batch(cur, results)

                for r in results:
                    _log_track(r)
                    done_count, not_found, not_necess, quota, conn_error, error_count = _tally(
                        r, done_count, not_found, not_necess, quota, conn_error, error_count
                    )

                conn.commit()

                print(
                    f"[TrackDetail] batch xong | "
                    f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                    f"quota={quota} | conn_error={conn_error} | error={error_count}",
                    flush=True,
                )

        if completed_normally:
            print(
                f"[TrackDetail] ✅ Hoàn thành | "
                f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                f"quota={quota} | conn_error={conn_error} | error={error_count}",
                flush=True,
            )
        else:
            print(
                f"[TrackDetail] 💾 Dừng giữa chừng | "
                f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                f"quota={quota} | conn_error={conn_error} | error={error_count}",
                flush=True,
            )

    except Exception as e:
        conn.rollback()
        print(f"[TrackDetail] ⚠️  Lỗi: {e}", flush=True)
        raise

    finally:
        cur.close()
        conn.close()


def _tally(r: dict, done: int, not_found: int, not_necess: int,
           quota: int, conn_error: int, error: int) -> tuple:
    s = r["scrape_status"]
    if   s == "done":       done       += 1
    elif s == "not_found":  not_found  += 1
    elif s == "not_necess": not_necess += 1
    elif s == "quota":      quota      += 1
    elif s == "conn_error": conn_error += 1
    else:                   error      += 1
    return done, not_found, not_necess, quota, conn_error, error


def _log_track(r: dict):
    status = r["scrape_status"]
    tid    = r["id"]
    if status == "done":
        print(f"[TrackDetail] ✓ [{tid}] {r.get('title')}", flush=True)
    elif status == "not_found":
        print(f"[TrackDetail] ~ [{tid}] not_found", flush=True)
    elif status == "not_necess":
        print(f"[TrackDetail] ~ [{tid}] not_necess", flush=True)
    elif status == "quota":
        print(f"[TrackDetail] ⚠️  [{tid}] quota — sẽ retry sau", flush=True)
    elif status == "conn_error":
        print(f"[TrackDetail] ⚠️  [{tid}] conn_error — sẽ retry sau", flush=True)
    else:
        print(f"[TrackDetail] ✗ [{tid}] {status}", flush=True)