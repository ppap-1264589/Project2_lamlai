import requests
import time
from config import ARTIST_URL, RATE_LIMIT_DELAY

def fetch_artist(artist_id: int) -> dict | None:
    resp = requests.get(ARTIST_URL.format(id=artist_id), timeout=5)
    time.sleep(RATE_LIMIT_DELAY)
    data = resp.json()
    if "error" in data:
        return None
    return {
        "id":       data["id"],
        "name":     data.get("name"),
        "nb_album": data.get("nb_album"),
        "nb_fan":   data.get("nb_fan"),
    }

def save_artist(cur, artist: dict):
    cur.execute("""
        INSERT INTO artists (id, name, nb_album, nb_fan)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (
        artist["id"],
        artist["name"],
        artist["nb_album"],
        artist["nb_fan"],
    ))