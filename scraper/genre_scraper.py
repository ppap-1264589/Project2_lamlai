import signal
import time

import requests

from config import GENRE_RATE_LIMIT_DELAY, GENRE_URL, HEADERS
from db import get_connection, setup_tables


PROGRESS_KEY = "genre"


def fetch_genre(genre_id: int) -> dict | None:
    resp = requests.get(GENRE_URL.format(id=genre_id), timeout=5, headers=HEADERS)
    time.sleep(GENRE_RATE_LIMIT_DELAY)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        return None

    name = data.get("name")
    if not name:
        return None

    return {
        "id": data.get("id") or genre_id,
        "name": name,
    }


def sync_pending_genres(cur) -> int:
    cur.execute("""
        INSERT INTO genres (id, name)
        SELECT DISTINCT genre_id, NULL
        FROM albums
        WHERE genre_id IS NOT NULL
        ON CONFLICT (id) DO NOTHING
    """)
    return cur.rowcount


def count_pending_genres(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM genres WHERE name IS NULL")
    return cur.fetchone()[0]


def get_progress(cur) -> int:
    cur.execute("SELECT last_id FROM scrape_progress WHERE scraper = %s", (PROGRESS_KEY,))
    row = cur.fetchone()
    return row[0] if row else 0


def save_progress(cur, last_id: int):
    cur.execute("""
        INSERT INTO scrape_progress (scraper, last_id) VALUES (%s, %s)
        ON CONFLICT (scraper) DO UPDATE SET last_id = EXCLUDED.last_id
    """, (PROGRESS_KEY, last_id))


def get_next_pending_genre_id(cur, last_id: int) -> int | None:
    cur.execute("""
        SELECT id
        FROM genres
        WHERE name IS NULL
          AND id > %s
        ORDER BY id
        LIMIT 1
    """, (last_id,))
    row = cur.fetchone()
    return row[0] if row else None


def save_genre(cur, genre: dict):
    cur.execute("""
        INSERT INTO genres (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, genres.name)
    """, (
        genre["id"],
        genre["name"],
    ))


def run_genre_scrape():
    conn = get_connection()
    setup_tables(conn)
    cur = conn.cursor()

    running = True

    def handle_stop(signum, frame):
        nonlocal running
        print("\nStopping genre scrape...")
        running = False

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    fetched = 0
    not_found = 0
    completed_pass = False

    try:
        inserted_pending = sync_pending_genres(cur)
        conn.commit()

        last_id = get_progress(cur)
        pending_total = count_pending_genres(cur)

        print(
            f"Starting genre scrape from ID > {last_id} "
            f"| pending synced: {inserted_pending} "
            f"| pending total: {pending_total}"
        )

        while running:
            genre_id = get_next_pending_genre_id(cur, last_id)

            if genre_id is None:
                completed_pass = True
                break

            try:
                genre = fetch_genre(genre_id)

                if genre:
                    save_genre(cur, genre)
                    fetched += 1
                    print(f"[genre {genre_id}] saved: {genre['name']}")
                else:
                    not_found += 1
                    print(f"[genre {genre_id}] not found; leaving name NULL")

                last_id = genre_id
                save_progress(cur, last_id)
                conn.commit()

            except Exception as e:
                conn.rollback()
                print(f"Warning: failed at genre {genre_id}: {e}")
                break

        if completed_pass:
            save_progress(cur, 0)
            conn.commit()

        remaining = count_pending_genres(cur)
        print(
            f"Genre scrape done | fetched: {fetched} "
            f"| not found this pass: {not_found} "
            f"| remaining NULL names: {remaining}"
        )

    finally:
        cur.close()
        conn.close()
