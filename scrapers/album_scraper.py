import requests
import time
from config import ARTIST_ALBUMS, ALBUM_URL, RATE_LIMIT_DELAY, HEADERS


def fetch_albums(artist_id: int) -> list[dict]:
    """Fetch danh sách album của artist (chỉ có metadata cơ bản, chưa có contributors)."""
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


def fetch_album_detail(album_id: int) -> tuple[dict | None, list[dict]]:
    """Fetch full detail của một album từ /album/{id}.

    Trả về (album_dict, contributors) trong cùng 1 lần gọi API
    — tránh gọi 2 lần cho cùng 1 album_id.
    """
    resp = requests.get(ALBUM_URL.format(id=album_id), timeout=5, headers=HEADERS)
    time.sleep(RATE_LIMIT_DELAY)
    data = resp.json()
    if "error" in data:
        return None, []

    album = {
        "id":           int(data["id"]),
        "title":        data.get("title"),
        "genre_id":     data.get("genre_id"),
        "release_date": data.get("release_date") or None,
        "record_type":  data.get("record_type"),
        "fans":         data.get("fans"),
    }
    contributors = [
        {"artist_id": c["id"], "role": c.get("role")}
        for c in data.get("contributors", [])
    ]
    return album, contributors


def save_album(cur, album: dict):
    # COALESCE thay vì DO NOTHING vì user_fav_track_scraper có thể đã insert
    # stub (chỉ có id) trước đó để đảm bảo FK album_tracks → albums không lỗi.
    # COALESCE fill các trường NULL của stub mà không ghi đè data đã có.
    cur.execute("""
        INSERT INTO albums (id, title, genre_id, release_date, record_type, fans)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            title        = COALESCE(albums.title,        EXCLUDED.title),
            genre_id     = COALESCE(albums.genre_id,     EXCLUDED.genre_id),
            release_date = COALESCE(albums.release_date, EXCLUDED.release_date),
            record_type  = COALESCE(albums.record_type,  EXCLUDED.record_type),
            fans         = COALESCE(albums.fans,         EXCLUDED.fans)
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