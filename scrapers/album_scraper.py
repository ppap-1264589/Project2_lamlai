import requests
import time
from config import ARTIST_ALBUMS, ALBUM_URL, RATE_LIMIT_DELAY, HEADERS

# DONE LOGIC
def fetch_albums(artist_id: int) -> list[dict]:
    """Fetch danh sách album của artist (chỉ có metadata cơ bản, chưa có contributors)."""
    albums = []
    url = ARTIST_ALBUMS.format(id=artist_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(RATE_LIMIT_DELAY)
        data = resp.json(content_type=None)
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            albums.append({
                "id":           item["id"],
                "title":        item.get("title"),
                "genre_id":     item.get("genre_id"),
                "duration":     item.get("duration"),
                "release_date": item.get("release_date") or None,
                "record_type":  item.get("record_type"),
                "fans":         item.get("fans"),
            })
        url = data.get("next")
    return albums

# DONE LOGIC
def fetch_album_ids(artist_id: int) -> list[int]:
    """Fetch danh sách id album của artist"""
    ids = []
    url = ARTIST_ALBUMS.format(id=artist_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(RATE_LIMIT_DELAY)
        data = resp.json(content_type=None)
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            ids.append(item["id"])
        url = data.get("next")
    return ids

# DONE LOGIC
def save_album_id(cur, album_id: int):
    cur.execute("""
        INSERT INTO albums (id)
        VALUES (%s)
        ON CONFLICT (id) DO NOTHING
    """, (album_id,))


# DONE LOGIC
def save_artist_album(cur, artist_id: int, album_id: int):
    cur.execute("""
        INSERT INTO artist_albums (artist_id, album_id)
        VALUES (%s, %s)
        ON CONFLICT (artist_id, album_id) DO NOTHING
    """, (artist_id, album_id))