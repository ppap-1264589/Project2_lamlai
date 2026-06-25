import requests
import time
from config import USER_FAV_ALBUMS, ALBUM_URL, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values


def fetch_fav_albums(user_id: int) -> list[dict]:
    """Fetch danh sách fav albums của user (có phân trang).
    Trả về list gồm album_id và time_add — chưa có detail.
    """
    entries = []
    url = USER_FAV_ALBUMS.format(id=user_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(USER_RATE_LIMIT_DELAY)
        data = resp.json()
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            entries.append({
                "album_id": int(item["id"]),
                "time_add": item.get("time_add"),
            })
        url = data.get("next")
    return entries


def fetch_album_detail(album_id: int) -> dict | None:
    """Fetch thông tin đầy đủ của một album từ /album/{id}."""
    resp = requests.get(ALBUM_URL.format(id=album_id), timeout=5, headers=HEADERS)
    time.sleep(USER_RATE_LIMIT_DELAY)
    data = resp.json()
    if "error" in data:
        return None
    return {
        "id":           int(data["id"]),
        "title":        data.get("title"),
        "genre_id":     data.get("genre_id"),
        "release_date": data.get("release_date") or None,
        "record_type":  data.get("record_type"),
        "fans":         data.get("fans"),
    }


def save_fav_albums(cur, user_id: int, albums: list[dict]):
    """Upsert album vào bảng albums, rồi insert user_fav_albums.

    albums: list dict có đầy đủ các trường từ fetch_album_detail
            + time_add từ fetch_fav_albums.

    Dùng COALESCE thay vì DO NOTHING vì:
    - save_fav_tracks có thể đã insert stub (id only) trước đó do FK constraint
    - Cần fill các trường NULL của stub đó khi có data đầy đủ
    - Nếu artist scraper đã insert đầy đủ rồi thì COALESCE giữ nguyên (không ghi đè)

    Dedup theo id trước khi insert để tránh lỗi:
    "ON CONFLICT DO UPDATE command cannot affect row a second time"
    — xảy ra khi một user fav nhiều track từ cùng một album,
      khiến albums_full có duplicate album_id trong cùng một batch.
    """
    if not albums:
        return

    # Dedup theo album id — giữ lần xuất hiện đầu tiên
    seen: dict[int, dict] = {}
    for a in albums:
        if a["id"] not in seen:
            seen[a["id"]] = a
    deduped = list(seen.values())

    album_rows = [
        (a["id"], a["title"], a["genre_id"], a["release_date"], a["record_type"], a["fans"])
        for a in deduped
    ]
    execute_values(cur, """
        INSERT INTO albums (id, title, genre_id, release_date, record_type, fans)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            title        = COALESCE(EXCLUDED.title,        albums.title),
            genre_id     = COALESCE(EXCLUDED.genre_id,     albums.genre_id),
            release_date = COALESCE(EXCLUDED.release_date, albums.release_date),
            record_type  = COALESCE(EXCLUDED.record_type,  albums.record_type),
            fans         = COALESCE(EXCLUDED.fans,         albums.fans)
    """, album_rows)

    # fav_rows giữ nguyên toàn bộ albums (không dedup) vì mỗi cặp
    # (user_id, album_id) là unique — DO NOTHING xử lý trùng lặp
    fav_rows = [
        (user_id, a["id"], a["time_add"])
        for a in albums
    ]
    execute_values(cur, """
        INSERT INTO user_fav_albums (user_id, album_id, time_add)
        VALUES %s
        ON CONFLICT (user_id, album_id) DO NOTHING
    """, fav_rows, template="(%s, %s, to_timestamp(%s))")