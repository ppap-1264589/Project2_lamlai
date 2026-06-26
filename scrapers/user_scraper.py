import requests
import time
from config import USER_URL, USER_RATE_LIMIT_DELAY, HEADERS

# DONE LOGIC
def fetch_user(user_id: int) -> dict | None:
    resp = requests.get(USER_URL.format(id=user_id), timeout=5, headers=HEADERS)
    time.sleep(USER_RATE_LIMIT_DELAY)
    data = resp.json(content_type=None)
    if "error" in data:
        return None
    return {
        "id":        user_id,
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

# DONE LOGIC
def save_user(cur, user: dict):
    cur.execute("""
        INSERT INTO users (id, name, lastname, firstname, email, birthday, gender, country, lang, is_kid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (user["id"], user["name"], user["lastname"], user["firstname"],
          user["email"], user["birthday"], user["gender"],
          user["country"], user["lang"], user["is_kid"]))