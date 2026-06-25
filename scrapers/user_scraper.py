import requests
import time
from config import USER_URL, USER_FAV_TRACKS, USER_RATE_LIMIT_DELAY, HEADERS
from psycopg2.extras import execute_values

def fetch_user(user_id: int) -> dict | None:
    resp = requests.get(USER_URL.format(id=user_id), timeout=5, headers=HEADERS)
    time.sleep(USER_RATE_LIMIT_DELAY)
    data = resp.json()
    if "error" in data:
        return None
    return {
        "id":        data["id"],
        "name":      data.get("name"),
        "lastname":  data.get("lastname"),
        "firstname": data.get("firstname"),
        "email":     data.get("email"),
        "birthday":  data.get("birthday") or None,
        "gender":    data.get("gender"),
        "country":   data.get("country"),
        "lang":      data.get("lang"),
        "is_kid":    data.get("is_kid"),
    }

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

def save_user(cur, user: dict):
    cur.execute("""
        INSERT INTO users (id, name, lastname, firstname, email, birthday, gender, country, lang, is_kid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (user["id"], user["name"], user["lastname"], user["firstname"],
          user["email"], user["birthday"], user["gender"],
          user["country"], user["lang"], user["is_kid"]))

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
        execute_values(cur, """
            INSERT INTO albums (id)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, [(album_id,) for album_id in {r[0] for r in album_track_rows}])

        execute_values(cur, """
            INSERT INTO album_tracks (album_id, track_id, track_position)
            VALUES %s
            ON CONFLICT (album_id, track_id) DO NOTHING
        """, [(album_id, track_id, None) for album_id, track_id in album_track_rows])