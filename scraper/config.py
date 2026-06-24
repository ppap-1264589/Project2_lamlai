BASE_URL = "https://api.deezer.com"

ARTIST_URL    = BASE_URL + "/artist/{id}"
ARTIST_ALBUMS = BASE_URL + "/artist/{id}/albums"
ALBUM_URL     = BASE_URL + "/album/{id}"
ALBUM_TRACKS  = BASE_URL + "/album/{id}/tracks"

RATE_LIMIT_DELAY = 0.2  # giây giữa mỗi request (5 req/s)