import requests
import time
from config import USER_FAV_TRACKS, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values


def fetch_fav_tracks(user_id: int) -> list[dict]:
    tracks = []
    url = USER_FAV_TRACKS.format(id=user_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(USER_RATE_LIMIT_DELAY)
        data = resp.json()
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            tracks.append({
                "track_id": item["id"],
                "title":    item.get("title"),
                "duration": item.get("duration"),
                "rank":     item.get("rank"),
                "time_add": item.get("time_add"),
                "album_id": item.get("album", {}).get("id"),
            })
        url = data.get("next")
    return tracks


def save_fav_tracks(cur, user_id: int, tracks: list[dict]):
    if not tracks:
        return

    track_rows_by_id = {}
    fav_rows = []
    album_track_rows = set()

    for track in tracks:
        track_id = track["track_id"]
        album_id = track["album_id"]
        track_rows_by_id.setdefault(
            track_id,
            (track_id, track["title"], track["duration"], track["rank"]),
        )
        fav_rows.append((user_id, track_id, track["time_add"]))
        if album_id:
            album_track_rows.add((album_id, track_id))

    execute_values(cur, """
        INSERT INTO tracks (id, title, duration, rank)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """, list(track_rows_by_id.values()))

    execute_values(cur, """
        INSERT INTO user_fav_tracks (user_id, track_id, time_add)
        VALUES %s
        ON CONFLICT (user_id, track_id) DO NOTHING
    """, fav_rows, template="(%s, %s, to_timestamp(%s))")

    if album_track_rows:
        # Insert stub album (chỉ id) để FK album_tracks → albums không lỗi.
        # Dùng DO NOTHING — nếu artist scraper đã insert album đầy đủ rồi thì giữ nguyên.
        # Nếu chưa có, tạo stub rỗng; save_fav_albums sẽ fill đầy đủ sau bằng COALESCE.
        # album_ids đã là set nên không có duplicate → không gây lỗi DO UPDATE.
        unique_album_ids = {r[0] for r in album_track_rows}
        execute_values(cur, """
            INSERT INTO albums (id)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, [(aid,) for aid in unique_album_ids])

        execute_values(cur, """
            INSERT INTO album_tracks (album_id, track_id, track_position)
            VALUES %s
            ON CONFLICT (album_id, track_id) DO NOTHING
        """, [(album_id, track_id, None) for album_id, track_id in album_track_rows])