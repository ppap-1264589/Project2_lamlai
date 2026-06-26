import requests
import time
from config import USER_FAV_ARTISTS, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values

# DONE LOGIC
def fetch_fav_artists(user_id: int) -> list[dict]:
    """Fetch tất cả nghệ sĩ yêu thích của một user (có phân trang)."""
    artists = []
    url = USER_FAV_ARTISTS.format(id=user_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(USER_RATE_LIMIT_DELAY)
        data = resp.json()
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            artists.append({
                "artist_id": int(item["id"]),
                "name":      item.get("name"),
                "nb_album":  item.get("nb_album"),
                "nb_fan":    item.get("nb_fan"),
                "time_add":  item.get("time_add"),
            })
        url = data.get("next")
    return artists

# DONE LOGIC
def save_fav_artists(cur, user_id: int, artists: list[dict]):
    """Lưu danh sách nghệ sĩ yêu thích vào DB.

    - Upsert artist cơ bản vào bảng artists (để FK không bị lỗi).
    - Insert quan hệ user ↔ artist vào user_fav_artists.
    """
    if not artists:
        return

    # Upsert artists (chỉ điền nếu chưa có)
    artist_rows = [
        (a["artist_id"], a["name"], a["nb_album"], a["nb_fan"])
        for a in artists
    ]
    execute_values(cur, """
        INSERT INTO artists (id, name, nb_album, nb_fan)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, artist_rows)

    # Insert user_fav_artists
    fav_rows = [
        (user_id, a["artist_id"], a["time_add"])
        for a in artists
    ]
    execute_values(cur, """
        INSERT INTO user_fav_artists (user_id, artist_id, time_add)
        VALUES %s
        ON CONFLICT (user_id, artist_id) DO NOTHING
    """, fav_rows, template="(%s, %s, to_timestamp(%s))")