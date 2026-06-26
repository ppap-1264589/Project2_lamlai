import requests
import time
from config import ALBUM_TRACKS, RATE_LIMIT_DELAY, HEADERS

# DONE LOGIC
def fetch_tracks_id(album_id: int) -> list[dict]:
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
            })
        url = data.get("next")
    return tracks

# DONE LOGIC
def save_tracks_id(cur, album_id: int, tracks: list[dict]):
    for track in tracks:
        # Mặc định thì trường scrape status là pending
        cur.execute("""
            INSERT INTO tracks (id)
            VALUES (%s)
            ON CONFLICT (id) DO NOTHING
        """, (track["id"],))