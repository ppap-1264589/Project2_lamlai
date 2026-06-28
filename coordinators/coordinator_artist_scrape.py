import signal
from db import get_connection, setup_tables
from scrapers.artist_scraper import fetch_artist, save_artist
from scrapers.album_scraper import fetch_album_ids, save_album_id, save_artist_album

PROGRESS_KEY = "artist"

# DONE LOGIC
def run_artist_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
    row = cur.fetchone()
    last_id = row[0] if row else 0

    cur.execute("SELECT COUNT(*) FROM artists")
    total_db = cur.fetchone()[0]
    print(f"[Artist] ▶️  Tiếp tục từ ID > {last_id} | Đang có {total_db} artists trong DB", flush=True)

    session_found = 0
    artist_id = last_id + 1
    running = True








    # docker stop
    #   → gửi SIGTERM
    #   → handle_stop() chạy, running = False
    #   → vòng lặp kết thúc sau batch hiện tại
    #   → container thoát sạch ✓

    # Ctrl+C gửi SIGINT
    #  → handle_stop() chạy, running = False
    #  → vòng lặp kết thúc sau batch hiện tại
    #  → container thoát sạch ✓

    # Nếu quá 60s vẫn chưa thoát:
    #   → Docker gửi SIGKILL (không thể catch)
    #   → kill cứng

    def handle_stop(signum, frame):
        nonlocal running
        print("\n[Artist] ⛔ Nhận tín hiệu dừng, đang chờ tác vụ hiện tại...", flush=True)
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)








    try:
        while running:
            success = False
            try:
                artist = fetch_artist(artist_id)
                if artist:
                    save_artist(cur, artist)
                    conn.commit()

                    if not running:
                        break

                    album_ids = fetch_album_ids(artist_id)
                    stop_requested = False

                    for aid in album_ids:
                        if not running:
                            stop_requested = True
                            break

                        save_album_id(cur, aid)
                        save_artist_album(cur, artist_id, aid)

                    if stop_requested:
                        conn.rollback()
                        print(f"[Artist] ⛔ Dừng giữa chừng tại ID {artist_id} — album chưa đủ, sẽ cào lại lần sau.", flush=True)
                        break
                    else:
                        conn.commit()

                    session_found += 1
                    total_db += 1
                    print(
                        f"[Artist] [ID {artist_id}] {artist['name']} "
                        f"— {artist['nb_fan']:,} fans | {len(album_ids)} albums "
                        f"| phiên này: {session_found} | tổng DB: {total_db}",
                        flush=True,
                    )

                success = True  # kể cả artist_id không tồn tại (fetch trả None) cũng skip bình thường

            except Exception as e:
                if running:
                    print(f"[Artist]   ⚠️  Lỗi artist_id {artist_id}: {e}", flush=True)
                conn.rollback()

            if success:
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, artist_id))
                conn.commit()
                artist_id += 1
            # nếu lỗi: không tăng artist_id, retry vòng tiếp theo

    finally:
        print(f"[Artist] ✅ Phiên này: {session_found} artists | Tổng DB: {total_db}", flush=True)
        cur.close()
        conn.close()