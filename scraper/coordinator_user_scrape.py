import signal

from db import get_connection, setup_tables
from user_scraper import fetch_fav_tracks, fetch_user, save_fav_tracks, save_user


def run_user_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = 'user'")
    row = cur.fetchone()
    start_id = (row[0] + 1) if row else 1

    cur.execute("SELECT COUNT(*) FROM users")
    total_db = cur.fetchone()[0]

    print(f"▶️  Tiếp tục từ user_id {start_id} | Đang có {total_db} users trong DB")

    session_found = 0
    user_id = start_id
    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\n⛔ Nhận tín hiệu dừng...")
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        while running:
            try:
                user = fetch_user(user_id)

                if user:
                    save_user(cur, user)
                    conn.commit()

                    tracks = fetch_fav_tracks(user_id)
                    save_fav_tracks(cur, user_id, tracks)
                    conn.commit()

                    session_found += 1
                    total_db += 1
                    print(
                        f"[ID {user_id}] {user['name']} ({user['country']}) "
                        f"— {len(tracks)} fav tracks "
                        f"| phiên này: {session_found} "
                        f"| tổng DB: {total_db}"
                    )

            except Exception as e:
                if running:
                    print(f"  ⚠️  Lỗi user_id {user_id}: {e}")
                conn.rollback()

            user_id += 1

    finally:
        cur.execute("""
            INSERT INTO scrape_progress (scraper, last_id) VALUES ('user', %s)
            ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
        """, (user_id,))
        conn.commit()
        print(f"💾 Đã lưu tiến độ tại user_id {user_id}")
        print(f"✅ Phiên này: {session_found} users mới | Tổng DB: {total_db}")
        cur.close()
        conn.close()
