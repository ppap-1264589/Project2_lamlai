import os

BASE_URL = "https://api.deezer.com"

ARTIST_URL    = BASE_URL + "/artist/{id}"
ARTIST_ALBUMS = BASE_URL + "/artist/{id}/albums"
ALBUM_URL     = BASE_URL + "/album/{id}"
ALBUM_TRACKS  = BASE_URL + "/album/{id}/tracks"

USER_URL        = BASE_URL + "/user/{id}"
USER_FAV_TRACKS = BASE_URL + "/user/{id}/tracks"

# Rate limits
ARTIST_REQUESTS_PER_SECOND = float(os.getenv("ARTIST_REQUESTS_PER_SECOND", "6.5"))
USER_REQUESTS_PER_SECOND = float(os.getenv("USER_REQUESTS_PER_SECOND", "2.5"))

rate_limits = {
    "artist_scraper": ARTIST_REQUESTS_PER_SECOND,
    "user_scraper":   USER_REQUESTS_PER_SECOND,
}

RATE_LIMIT_DELAY      = 1 / rate_limits["artist_scraper"]  # ~0.154s
USER_RATE_LIMIT_DELAY = 1 / rate_limits["user_scraper"]    # ~0.400s
