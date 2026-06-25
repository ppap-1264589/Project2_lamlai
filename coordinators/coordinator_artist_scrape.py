import signal
from db import get_connection, setup_tables
from scrapers.artist_scraper import fetch_artist, save_artist
from scrapers.album_scraper import fetch_albums, fetch_contributors, save_album, save_artist_album
from scrapers.track_scraper import fetch_tracks, save_tracks

def run_artist_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = 'artist'")
    row = cur.fetchone()
    start_id = (row[0] + 1) if row else 1

    cur.execute("SELECT COUNT(*) FROM artists")
    total_db = cur.fetchone()[0]
    print(f"[Artist] ▶️  Tiếp tục từ ID {start_id} | Đang có {total_db} artists trong DB")

    session_found = 0
    artist_id = start_id
    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\n[Artist] ⛔ Nhận tín hiệu dừng...")
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        while running:
            try:
                artist = fetch_artist(artist_id)
                if artist:
                    save_artist(cur, artist)
                    conn.commit()

                    albums = fetch_albums(artist_id)
                    for album in albums:
                        save_album(cur, album)
                        contributors = fetch_contributors(album["id"])
                        save_artist_album(cur, artist_id, album["id"], contributors)
                        tracks = fetch_tracks(album["id"])
                        save_tracks(cur, album["id"], tracks)
                    conn.commit()

                    session_found += 1
                    total_db += 1
                    print(
                        f"[Artist] [ID {artist_id}] {artist['name']} "
                        f"— {artist['nb_fan']:,} fans | {len(albums)} albums "
                        f"| phiên này: {session_found} | tổng DB: {total_db}"
                    )
            except Exception as e:
                if running:
                    print(f"[Artist]   ⚠️  Lỗi ID {artist_id}: {e}")
                conn.rollback()

            artist_id += 1

    finally:
        cur.execute("""
            INSERT INTO scrape_progress (scraper, last_id) VALUES ('artist', %s)
            ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
        """, (artist_id,))
        conn.commit()
        print(f"[Artist] 💾 Đã lưu tiến độ tại ID {artist_id}")
        print(f"[Artist] ✅ Phiên này: {session_found} artists | Tổng DB: {total_db}")
        cur.close()
        conn.close()