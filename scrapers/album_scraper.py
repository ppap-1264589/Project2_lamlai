import requests
import time
from config import ARTIST_ALBUMS, ALBUM_URL, RATE_LIMIT_DELAY, HEADERS

def fetch_albums(artist_id: int) -> list[dict]:
    albums = []
    url = ARTIST_ALBUMS.format(id=artist_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(RATE_LIMIT_DELAY)
        data = resp.json()
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            albums.append({
                "id":           item["id"],
                "title":        item.get("title"),
                "genre_id":     item.get("genre_id"),
                "release_date": item.get("release_date") or None,
                "record_type":  item.get("record_type"),
                "fans":         item.get("fans"),
            })
        url = data.get("next")
    return albums

def fetch_contributors(album_id: int) -> list[dict]:
    resp = requests.get(ALBUM_URL.format(id=album_id), timeout=5, headers=HEADERS)
    time.sleep(RATE_LIMIT_DELAY)
    data = resp.json()
    if "error" in data or "contributors" not in data:
        return []
    return [{"artist_id": c["id"], "role": c.get("role")} for c in data["contributors"]]

def save_album(cur, album: dict):
    cur.execute("""
        INSERT INTO albums (id, title, genre_id, release_date, record_type, fans)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            title        = COALESCE(EXCLUDED.title,        albums.title),
            genre_id     = COALESCE(EXCLUDED.genre_id,     albums.genre_id),
            release_date = COALESCE(EXCLUDED.release_date, albums.release_date),
            record_type  = COALESCE(EXCLUDED.record_type,  albums.record_type),
            fans         = COALESCE(EXCLUDED.fans,         albums.fans)
    """, (album["id"], album["title"], album["genre_id"],
          album["release_date"], album["record_type"], album["fans"]))

def save_artist_album(cur, artist_id: int, album_id: int, contributors: list[dict]):
    for c in contributors:
        if c["artist_id"] == artist_id:
            cur.execute("""
                INSERT INTO artist_albums (artist_id, album_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (artist_id, album_id) DO NOTHING
            """, (artist_id, album_id, c["role"]))
            break