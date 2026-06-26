# coordinator_user_scrape.py
import signal
from db import get_connection, setup_tables
from scrapers.user_scraper import fetch_user, save_user
from scrapers.user_fav_track_scraper import fetch_fav_tracks, save_fav_tracks
from scrapers.user_fav_artist_scraper import fetch_fav_artists, save_fav_artists
from scrapers.user_fav_album_scraper import fetch_fav_album, save_fav_albums

PROGRESS_KEY = "user"

def run_user_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
    row = cur.fetchone()
    last_id = row[0] if row else 0

    cur.execute("SELECT COUNT(*) FROM users")
    total_db = cur.fetchone()[0]
    print(f"[User] ▶️  Tiếp tục từ user_id > {last_id} | Đang có {total_db} users trong DB", flush=True)

    session_found = 0
    user_id = last_id + 1
    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\n[User] ⛔ Nhận tín hiệu dừng, đang chờ tác vụ hiện tại...", flush=True)
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        while running:
            success = False  # ← thêm flag
            try:
                user = fetch_user(user_id)
                if user:
                    save_user(cur, user)
                    conn.commit()

                    if not running:
                        break

                    tracks = fetch_fav_tracks(user_id)
                    save_fav_tracks(cur, user_id, tracks)
                    conn.commit()

                    if not running:
                        break

                    artists = fetch_fav_artists(user_id)
                    save_fav_artists(cur, user_id, artists)
                    conn.commit()

                    if not running:
                        break

                    albums = fetch_fav_album(user_id)
                    save_fav_albums(cur, user_id, albums)
                    conn.commit()

                    if not running:
                        break

                    session_found += 1
                    total_db += 1
                    print(
                        f"[User] [ID {user_id}] {user['name']} ({user['country']}) "
                        f"— tracks: {len(tracks)} | artists: {len(artists)} | albums: {len(albums)} "
                        f"| phiên này: {session_found} | tổng DB: {total_db}",
                        flush=True,
                    )

                success = True  # ← chỉ set True khi không có exception

            except Exception as e:
                if running:
                    print(f"[User]   ⚠️  Lỗi user_id {user_id}: {e}", flush=True)
                conn.rollback()
                # success vẫn là False → không lưu progress, không tăng user_id

            if success:  # ← chỉ lưu và tăng khi thành công
                cur.execute("""
                    INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
                    ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
                """, (PROGRESS_KEY, user_id))
                conn.commit()
                user_id += 1
            # nếu lỗi: không tăng user_id, retry vòng tiếp theo

    finally:
        print(f"[User] ✅ Phiên này: {session_found} users | Tổng DB: {total_db}", flush=True)
        cur.close()
        conn.close()