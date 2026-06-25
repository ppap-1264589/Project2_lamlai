import requests
import time
from config import GENRE_URL, GENRE_RATE_LIMIT_DELAY, HEADERS

def fetch_genre(genre_id: int) -> dict | None:
    resp = requests.get(GENRE_URL.format(id=genre_id), timeout=5, headers=HEADERS)
    time.sleep(GENRE_RATE_LIMIT_DELAY)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data or not data.get("name"):
        return None
    return {"id": data.get("id") or genre_id, "name": data["name"]}

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

def get_next_pending_genre_id(cur, last_id: int) -> int | None:
    cur.execute("""
        SELECT id FROM genres
        WHERE name IS NULL AND id > %s
        ORDER BY id LIMIT 1
    """, (last_id,))
    row = cur.fetchone()
    return row[0] if row else None

def save_genre(cur, genre: dict):
    cur.execute("""
        INSERT INTO genres (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, genres.name)
    """, (genre["id"], genre["name"]))