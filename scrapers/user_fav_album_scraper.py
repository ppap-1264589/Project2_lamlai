import requests
import time
from config import USER_FAV_ALBUMS, ALBUM_URL, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values

# DONE LOGIC
def fetch_fav_album(user_id: int) -> list[dict]:
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

# DONE LOGIC
def save_fav_albums(cur, user_id: int, albums: list[dict]):
    if not albums:
        return

    # Đổi a["id"] → a["album_id"]
    unique_album_ids = list({a["album_id"] for a in albums})

    execute_values(cur, """
        INSERT INTO albums (id)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, [(aid,) for aid in unique_album_ids])

    execute_values(cur, """
        INSERT INTO user_fav_albums (user_id, album_id, time_add)
        VALUES %s
        ON CONFLICT (user_id, album_id) DO NOTHING
    """, [(user_id, a["album_id"], a["time_add"]) for a in albums],
    template="(%s, %s, to_timestamp(%s))")
