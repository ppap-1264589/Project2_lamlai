import signal
from db import get_connection, setup_tables
from scrapers.genre_scraper import (
    sync_pending_genres, count_pending_genres,
    get_next_pending_genre_id, fetch_genre, save_genre,
)

PROGRESS_KEY = "genre"

def run_genre_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\n[Genre] ⛔ Nhận tín hiệu dừng...")
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

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

        fetched = not_found = 0

        while running:
            genre_id = get_next_pending_genre_id(cur, last_id)
            if genre_id is None:
                break
            try:
                genre = fetch_genre(genre_id)
                if genre:
                    save_genre(cur, genre)
                    fetched += 1
                    print(f"[Genre] [{genre_id}] {genre['name']}")
                else:
                    not_found += 1
                    print(f"[Genre] [{genre_id}] not found")

                last_id = genre_id
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, last_id))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"[Genre] ⚠️  Lỗi genre {genre_id}: {e}")
                break

        # Reset progress sau khi pass hoàn chỉnh
        cur.execute("""
            INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, 0)
            ON CONFLICT (scraper) DO UPDATE SET last_id = 0
        """, (PROGRESS_KEY,))
        conn.commit()
        print(f"[Genre] ✅ Xong | fetched: {fetched} | not found: {not_found}")

    finally:
        cur.close()
        conn.close()