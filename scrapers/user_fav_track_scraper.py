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
    album_ids = set()

    for track in tracks:
        track_id = track["track_id"]
        album_id = track["album_id"]
        track_rows_by_id.setdefault(
            track_id,
            (track_id, track["title"], track["duration"], track["rank"]),
        )
        fav_rows.append((user_id, track_id, track["time_add"]))
        if album_id:
            album_ids.add(album_id)

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

    # Chỉ insert stub album_id — KHÔNG insert album_tracks ở đây.
    # album_tracks (với track_position đầy đủ) sẽ được lấy sau
    # bởi coordinator_album_detail_scrape qua API /album/{id}/tracks.
    if album_ids:
        execute_values(cur, """
            INSERT INTO albums (id)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, [(aid,) for aid in album_ids])