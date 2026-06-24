import os

BASE_URL = "https://api.deezer.com"

ARTIST_URL    = BASE_URL + "/artist/{id}"
ARTIST_ALBUMS = BASE_URL + "/artist/{id}/albums"
ALBUM_URL     = BASE_URL + "/album/{id}"
ALBUM_TRACKS  = BASE_URL + "/album/{id}/tracks"
GENRE_URL     = BASE_URL + "/genre/{id}"

USER_URL        = BASE_URL + "/user/{id}"
USER_FAV_TRACKS = BASE_URL + "/user/{id}/tracks"

HEADERS = {
    'Accept-Language': 'en-US'
}

# Rate limits
ARTIST_REQUESTS_PER_SECOND = float(os.getenv("ARTIST_REQUESTS_PER_SECOND", "6.5"))
USER_REQUESTS_PER_SECOND = float(os.getenv("USER_REQUESTS_PER_SECOND", "3"))
GENRE_REQUESTS_PER_SECOND = float(os.getenv("GENRE_REQUESTS_PER_SECOND", "10"))

rate_limits = {
    "artist_scraper": ARTIST_REQUESTS_PER_SECOND,
    "user_scraper":   USER_REQUESTS_PER_SECOND,
    "genre_scraper":  GENRE_REQUESTS_PER_SECOND,
}

RATE_LIMIT_DELAY      = 1 / rate_limits["artist_scraper"]  # ~0.154s
USER_RATE_LIMIT_DELAY = 1 / rate_limits["user_scraper"]    # ~0.400s
GENRE_RATE_LIMIT_DELAY = 1 / rate_limits["genre_scraper"]  # ~0.100s
