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
from config import GENRE_DETAIL_REQUESTS_PER_SECOND

CONCURRENCY = 50


def run_genre_detail_scrape():
    asyncio.run(_run())


async def _run():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    # stop_event.is_set() == False  →  chưa nhận tín hiệu dừng
    # stop_event.set()              →  bật cờ
    # stop_event.is_set() == True   →  đã nhận tín hiệu dừng
    # stop_event.wait()             →  coroutine chờ đến khi cờ được bật
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_stop():
        print("\n[Genre] ⛔ Nhận tín hiệu dừng...", flush=True)
        stop_event.set()

    import signal
    loop.add_signal_handler(signal.SIGTERM, handle_stop)
    loop.add_signal_handler(signal.SIGINT,  handle_stop)

    try:
        inserted = sync_pending_genres(cur)
        conn.commit()

        pending = count_pending_genres(cur)
        print(f"[Genre] pending: {pending} | synced mới: {inserted}", flush=True)

        if pending == 0:
            print("[Genre] ✅ Không có gì cần cào, thoát.", flush=True)
            return

        # bucket — kiểm soát tốc độ theo thời gian
        # sem — giới hạn số request đồng thời
        bucket = TokenBucket(GENRE_DETAIL_REQUESTS_PER_SECOND)
        sem    = asyncio.Semaphore(CONCURRENCY)

        done_count = not_found = not_necess = quota = conn_error = error_count = 0
        completed_normally = False

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                genre_ids = get_pending_genre_ids(cur, BATCH_SIZE)
                if not genre_ids:
                    completed_normally = True
                    break

                fetch_task = asyncio.create_task(fetch_batch(session, genre_ids, bucket, sem))
                stop_task  = asyncio.create_task(stop_event.wait())

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


                # Xử lý trường hợp hy hữu
                # Task fetch_task có thể đã hoàn thành nhưng stop_event.is_set() == True, 
                # (Trường hợp cả hai task hoàn thành gần như cùng một lúc và stop_event được set trước khi fetch_task.result() được gọi)
                if stop_event.is_set():
                    if fetch_task in done and not fetch_task.cancelled():
                        try:
                            results = fetch_task.result()
                            for r in results:
                                save_genre(cur, r)
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    break

                results = fetch_task.result()
                for r in results:
                    _log_genre(r)
                    save_genre(cur, r)
                    done_count, not_found, not_necess, quota, conn_error, error_count = _tally(
                        r, done_count, not_found, not_necess, quota, conn_error, error_count
                    )
                conn.commit()

                print(
                    f"[Genre] batch xong | "
                    f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                    f"quota={quota} | conn_error={conn_error} | error={error_count}",
                    flush=True,
                )

        if completed_normally:
            conn.commit()
            print(
                f"[Genre] ✅ Hoàn thành | "
                f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                f"quota={quota} | conn_error={conn_error} | error={error_count}",
                flush=True,
            )
        else:
            print(
                f"[Genre] 💾 Dừng giữa chừng | "
                f"done={done_count} | not_found={not_found} | not_necess={not_necess} | "
                f"quota={quota} | conn_error={conn_error} | error={error_count}",
                flush=True,
            )

    except Exception as e:
        conn.rollback()
        print(f"[Genre] ⚠️  Lỗi: {e}", flush=True)
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


def _log_genre(r: dict):
    status = r["scrape_status"]
    gid    = r["id"]
    if status == "done":
        print(f"[Genre] ✓ [{gid}] {r.get('name')}", flush=True)
    elif status == "not_found":
        print(f"[Genre] ~ [{gid}] not_found", flush=True)
    elif status == "not_necess":
        print(f"[Genre] ~ [{gid}] not_necess", flush=True)
    elif status == "quota":
        print(f"[Genre] ⚠️  [{gid}] quota — sẽ retry sau", flush=True)
    elif status == "conn_error":
        print(f"[Genre] ⚠️  [{gid}] conn_error — sẽ retry sau", flush=True)
    else:
        print(f"[Genre] ✗ [{gid}] {status}", flush=True)