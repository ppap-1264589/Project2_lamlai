import os

BASE_URL = "https://api.deezer.com"

ARTIST_URL      = BASE_URL + "/artist/{id}"
ARTIST_ALBUMS   = BASE_URL + "/artist/{id}/albums"
ALBUM_URL       = BASE_URL + "/album/{id}"
ALBUM_TRACKS    = BASE_URL + "/album/{id}/tracks"
GENRE_URL       = BASE_URL + "/genre/{id}"
TRACK_URL       = BASE_URL + "/track/{id}"
USER_URL        = BASE_URL + "/user/{id}"
USER_FAV_TRACKS  = BASE_URL + "/user/{id}/tracks"
USER_FAV_ARTISTS = BASE_URL + "/user/{id}/artists"
USER_FAV_ALBUMS  = BASE_URL + "/user/{id}/albums"

HEADERS = {
    "Accept-Language": "en-US",
}

# Rate limits (configurable via env)
ARTIST_REQUESTS_PER_SECOND       = float(os.getenv("ARTIST_REQUESTS_PER_SECOND", "6.5"))
USER_REQUESTS_PER_SECOND         = float(os.getenv("USER_REQUESTS_PER_SECOND", "3"))
GENRE_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("GENRE_DETAIL_REQUESTS_PER_SECOND", "10"))
TRACK_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("TRACK_DETAIL_REQUESTS_PER_SECOND", "10"))
ALBUM_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("ALBUM_DETAIL_REQUESTS_PER_SECOND", "10"))

RATE_LIMIT_DELAY              = 1 / ARTIST_REQUESTS_PER_SECOND
USER_RATE_LIMIT_DELAY         = 1 / USER_REQUESTS_PER_SECOND
GENRE_DETAIL_RATE_LIMIT_DELAY = 1 / GENRE_DETAIL_REQUESTS_PER_SECOND
TRACK_DETAIL_RATE_LIMIT_DELAY = 1 / TRACK_DETAIL_REQUESTS_PER_SECOND
ALBUM_DETAIL_RATE_LIMIT_DELAY = 1 / ALBUM_DETAIL_REQUESTS_PER_SECOND

# Job scraper sleep interval (seconds) khi hết việc
JOB_SLEEP_SECONDS = int(os.getenv("JOB_SLEEP_SECONDS", "1800"))  # 30 phút