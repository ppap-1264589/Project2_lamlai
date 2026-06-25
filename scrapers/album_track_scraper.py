import requests
import time
from config import ALBUM_TRACKS, RATE_LIMIT_DELAY, HEADERS

def fetch_tracks(album_id: int) -> list[dict]:
    tracks = []
    url = ALBUM_TRACKS.format(id=album_id)
    while url:
        resp = requests.get(url, timeout=5, headers=HEADERS)
        time.sleep(RATE_LIMIT_DELAY)
        data = resp.json()
        if "error" in data or "data" not in data:
            break
        for item in data["data"]:
            tracks.append({
                "id":             item["id"],
                "title":          item.get("title"),
                "duration":       item.get("duration"),
                "rank":           item.get("rank"),
                "track_position": item.get("track_position"),
            })
        url = data.get("next")
    return tracks

def save_tracks(cur, album_id: int, tracks: list[dict]):
    for track in tracks:
        cur.execute("""
            INSERT INTO tracks (id, title, duration, rank)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (track["id"], track["title"], track["duration"], track["rank"]))

        cur.execute("""
            INSERT INTO album_tracks (album_id, track_id, track_position)
            VALUES (%s, %s, %s)
            ON CONFLICT (album_id, track_id) DO UPDATE SET
                track_position = COALESCE(album_tracks.track_position, EXCLUDED.track_position)
        """, (album_id, track["id"], track["track_position"]))