import signal
from db import get_connection, setup_tables
from scrapers.artist_scraper import fetch_artist, save_artist
from scrapers.album_scraper import fetch_albums, save_album, save_artist_album
from scrapers.album_track_scraper import fetch_tracks, save_tracks

PROGRESS_KEY = "artist"

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

                    albums = fetch_albums(artist_id)
                    stop_requested = False

                    for album in albums:
                        if not running:
                            stop_requested = True
                            break

                        save_album(cur, album)
                        save_artist_album(cur, artist_id, album["id"])
                        tracks = fetch_tracks(album["id"])
                        save_tracks(cur, album["id"], tracks)

                    conn.commit()

                    if stop_requested:
                        print(f"[Artist] ⛔ Dừng giữa chừng tại ID {artist_id} — sẽ cào lại lần sau.", flush=True)
                        break

                    session_found += 1
                    total_db += 1
                    print(
                        f"[Artist] [ID {artist_id}] {artist['name']} "
                        f"— {artist['nb_fan']:,} fans | {len(albums)} albums "
                        f"| phiên này: {session_found} | tổng DB: {total_db}",
                        flush=True,
                    )

                success = True  # kể cả artist_id không tồn tại (fetch trả None) cũng skip bình thường

            except Exception as e:
                if running:
                    print(f"[Artist]   ⚠️  Lỗi ID {artist_id}: {e}", flush=True)
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