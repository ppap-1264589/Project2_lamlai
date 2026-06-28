import os

BASE_URL = "https://api.deezer.com"

ARTIST_URL       = BASE_URL + "/artist/{id}"
ARTIST_ALBUMS    = BASE_URL + "/artist/{id}/albums"
ALBUM_URL        = BASE_URL + "/album/{id}"
ALBUM_TRACKS     = BASE_URL + "/album/{id}/tracks"
GENRE_URL        = BASE_URL + "/genre/{id}"
TRACK_URL        = BASE_URL + "/track/{id}"
USER_URL         = BASE_URL + "/user/{id}"
USER_FAV_TRACKS  = BASE_URL + "/user/{id}/tracks"
USER_FAV_ARTISTS = BASE_URL + "/user/{id}/artists"
USER_FAV_ALBUMS  = BASE_URL + "/user/{id}/albums"

HEADERS = {
    "Accept-Language": "en-US"
}

# Rate limits (configurable via env)

# 2 Job chính dùng thư viện đồng bộ, req trước phải chờ req sau trả về rồi mới cào tiếp được
# -> đặt số req/s thực tế có thể cao lên một chút
# Mặc định là 5+5 = 10req/s -> 0.1s round trip time
# Thực ra 0.1s này là sleep time, đảm bảo khó mà dính rate limit

# 3 Job riêng dùng thư viện bất đồng bộ, req trước không cần chờ req sau trả về
# -> đặt số req/s thực tế phải thấp hơn để tránh bị rate limit của Deezer API.
# Mặc định 9.5req/s đối với các job riêng, 

# Docker compose là nơi override trực tiếp giá trị của các biến môi trường này
# giảm đi để tránh bị rate limit của Deezer API. 
# tăng lên nếu muốn test rate limit cao hơn
ARTIST_REQUESTS_PER_SECOND       = float(os.getenv("ARTIST_REQUESTS_PER_SECOND", "5"))
USER_REQUESTS_PER_SECOND         = float(os.getenv("USER_REQUESTS_PER_SECOND", "5"))
GENRE_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("GENRE_DETAIL_REQUESTS_PER_SECOND", "9.5"))
TRACK_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("TRACK_DETAIL_REQUESTS_PER_SECOND", "9.5"))
ALBUM_DETAIL_REQUESTS_PER_SECOND = float(os.getenv("ALBUM_DETAIL_REQUESTS_PER_SECOND", "9.5"))

RATE_LIMIT_DELAY              = 1 / ARTIST_REQUESTS_PER_SECOND
USER_RATE_LIMIT_DELAY         = 1 / USER_REQUESTS_PER_SECOND
GENRE_DETAIL_RATE_LIMIT_DELAY = 1 / GENRE_DETAIL_REQUESTS_PER_SECOND
TRACK_DETAIL_RATE_LIMIT_DELAY = 1 / TRACK_DETAIL_REQUESTS_PER_SECOND
ALBUM_DETAIL_RATE_LIMIT_DELAY = 1 / ALBUM_DETAIL_REQUESTS_PER_SECOND



# BATCH_SIZE=100  →  tạo 100 task cùng lúc
#                         ↓
# sem=50          →  chỉ 50 cái được gửi request đồng thời
#                         ↓
# bucket=10/s     →  trong 50 cái đó, mỗi giây chỉ 10 cái
#                    được acquire token để thực sự gửi đi
BATCH_SIZE = 100
CONCURRENCY = 50