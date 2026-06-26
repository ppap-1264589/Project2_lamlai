import requests
import time
from config import USER_FAV_TRACKS, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values

# DONE LOGIC
def fetch_fav_tracks(user_id: int) -> list[dict]:
    tracks = []
    url = USER_FAV_TRACKS.format(id=user_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(USER_RATE_LIMIT_DELAY)
        data = resp.json(content_type=None)
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            tracks.append({
                "track_id": item["id"],
                "time_add": item.get("time_add"),
                "album_id": item.get("album", {}).get("id"),
            })
        url = data.get("next")
    return tracks


# DONE LOGIC
def save_fav_tracks(cur, user_id: int, tracks: list[dict]):
    if not tracks:
        return

    # 1. Deduplicate tracks
    unique_tracks = list({t["track_id"]: t for t in tracks}.values())

    # 2. Lấy album từ danh sách track, rồi deduplicate
    unique_album_ids = list({t["album_id"] for t in unique_tracks if t["album_id"]})

    # 3. Insert tracks, mặc định trong db thì scrape status lúc mới add id là pending
    execute_values(cur, """
        INSERT INTO tracks (id)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, [(t["track_id"],) for t in unique_tracks])

    # 4. Insert albums, mặc định trong db thì scrape status lúc mới add id là pending
    execute_values(cur, """
        INSERT INTO albums (id)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, [(aid,) for aid in unique_album_ids])

    # 5. Insert user_fav_tracks
    execute_values(cur, """
        INSERT INTO user_fav_tracks (user_id, track_id, time_add)
        VALUES %s
        ON CONFLICT (user_id, track_id) DO NOTHING
    """, [(user_id, t["track_id"], t["time_add"]) for t in tracks],
    template="(%s, %s, to_timestamp(%s))")