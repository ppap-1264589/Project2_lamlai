# 🎵 Deezer Data Scraper & Analytics Pipeline

Dự án thu thập dữ liệu quy mô lớn từ Deezer Public API, lưu trữ vào PostgreSQL và trực quan hóa bằng Apache Superset — toàn bộ chạy trong môi trường Docker.

---

## 📐 Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Compose                      │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐                   │
│  │   scraper    │   │   scraper    │                   │
│  │   (artist)   │   │    (user)    │                   │
│  │  6.5 req/s   │   │   3 req/s    │                   │
│  └──────┬───────┘   └──────┬───────┘                   │
│         │                  │                            │
│         ▼                  ▼                            │
│  ┌─────────────────────────────┐                       │
│  │         PostgreSQL          │                       │
│  │       (container)           │                       │
│  └─────────────┬───────────────┘                       │
│                │                                        │
│  ┌─────────────▼───────────────┐                       │
│  │    Apache Superset          │                       │
│  │       (container)           │                       │
│  └─────────────────────────────┘                       │
│                                                         │
│  ── Job phụ (--profile jobs) ──────────────────────    │
│  scraper_genre_detail  │  scraper_album_detail  │       │
│  scraper_track_detail                                   │
└─────────────────────────────────────────────────────────┘
```

---

## 🗄️ Schema Database (`db.py`)

| Bảng | Mô tả |
|---|---|
| `artists` | Thông tin nghệ sĩ: id, name, nb_album, nb_fan |
| `genres` | Thể loại nhạc, có trạng thái `scrape_status` |
| `albums` | Album: title, genre_id, release_date, record_type, fans |
| `artist_albums` | Quan hệ artist–album kèm role của contributor |
| `tracks` | Track: title, duration, rank, bpm, release_date |
| `album_tracks` | Quan hệ album–track kèm thứ tự track |
| `track_available_countries` | Các quốc gia mà track khả dụng |
| `users` | Người dùng: name, email, birthday, gender, country, lang |
| `user_fav_tracks` | Track yêu thích của user |
| `user_fav_artists` | Nghệ sĩ yêu thích của user |
| `user_fav_albums` | Album yêu thích của user |
| `scrape_progress` | Theo dõi tiến trình cào (last_id) của từng scraper |

---

## 🚀 Hướng dẫn chạy

### 1. Khởi động hệ thống chính

```bash
docker compose up -d
```

Lệnh này sẽ khởi động:
- Container **PostgreSQL** — lưu trữ dữ liệu
- Container **scraper_artist** — cào nghệ sĩ với rate limit 6.5 req/s
- Container **scraper_user** — cào người dùng với rate limit 3 req/s
- Container **Superset** — dashboard trực quan hóa

### 2. Khởi tạo Superset (chạy lần đầu)

Trước khi vào Superset, cần chạy script khởi tạo quyền admin một lần:

```bash
docker compose exec superset bash /app/superset/entrypoint.sh
```

### 3. Dừng hệ thống chính

```bash
docker compose down
```

---

## ⚙️ Chạy các Job phụ

Sau khi dữ liệu cơ bản đã được cào xong từ hai scraper chính, có thể dừng hệ thống lại và chạy 3 job phụ để bổ sung thông tin còn thiếu mà không cần cào lại từ đầu:

```bash
# Cào chi tiết thể loại nhạc
docker compose --profile jobs run -d --rm scraper_genre_detail

# Cào chi tiết album (genre, release_date,...)
docker compose --profile jobs run -d --rm scraper_album_detail

# Cào chi tiết track (bpm, release_date, available_countries)
docker compose --profile jobs run -d --rm scraper_track_detail
```

> Vì các job phụ chạy lần lượt (không song song với scraper chính), có thể đẩy rate lên **10 req/s** và sử dụng **async I/O** để tăng tốc độ.

---

## 🔍 Phương pháp cào dữ liệu

### Xác định vùng ID cần cào (User)

File `mapping_user_id/mapper.py` dùng phương pháp thống kê (coarse scan) để chứng minh rằng tỉ lệ user ID hữu ích trong khoảng **từ 6.9 tỷ trở đi gần như bằng 0**. Do đó, vùng cào được giới hạn từ **ID = 1 đến ~6.9 tỷ**, giúp tránh lãng phí tài nguyên.

### Thuật toán cào "tựa BFS"

**Đầu Artist (bắt đầu từ ID = 1):**
1. Gọi `GET /artist/{id}` → lưu thông tin artist vào bảng `artists`
2. Gọi `GET /artist/{id}/albums` → lấy danh sách album, lưu quan hệ vào `artist_albums`
3. Với mỗi album → gọi `GET /album/{id}/tracks` → lưu track + quan hệ vào `album_tracks`
4. Với mỗi album, extract thêm danh sách **contributor** → đưa ID của họ vào hàng đợi cào artist
5. Xác định được role của contributor trong từng album → lưu vào `artist_albums`

**Đầu User (bắt đầu từ ID = 1):**
1. Gọi `GET /user/{id}` → lưu thông tin vào bảng `users`
2. Gọi `GET /user/{id}/tracks` → lưu track yêu thích + phát hiện thêm album ID (chưa đầy đủ thông tin, sẽ được job phụ hoàn thiện)
3. Gọi `GET /user/{id}/artists` → lưu nghệ sĩ yêu thích
4. Gọi `GET /user/{id}/albums` → lưu album yêu thích

### Dữ liệu bổ sung qua Job phụ

| Job | Dữ liệu bổ sung |
|---|---|
| `scraper_track_detail` | `bpm`, `release_date`, `track_available_countries` |
| `scraper_album_detail` | `genre_id` còn trống trong các album |
| `scraper_genre_detail` | Tên và thông tin đầy đủ của genre |

---

## ⚡ Cấu hình Rate Limit (`config.py`)

API của Deezer giới hạn **50 req / 5 giây** (tức ~10 req/s).

| Scraper | Rate limit cài đặt | Lý do |
|---|---|---|
| scraper_artist | 6.5 req/s | Chạy song song với scraper_user |
| scraper_user | 3 req/s | Chạy song song với scraper_artist |
| Job phụ | 10 req/s | Chạy độc lập, async |

---

## 📁 Cấu trúc thư mục

```
Project2_lamlai/
│
├── config.py                        # Cấu hình rate limit, kết nối API
├── db.py                            # Định nghĩa schema và kết nối PostgreSQL
├── docker-compose.yml               # Orchestration toàn bộ hệ thống
├── dockerfile                       # Image cho các scraper container
├── main.py                          # Entrypoint chính
│
├── coordinators/                    # Điều phối luồng cào cho từng đối tượng
│   ├── coordinator_artist_scrape.py
│   ├── coordinator_user_scrape.py
│   ├── coordinator_album_detail_scrape.py
│   ├── coordinator_genre_detail_scrape.py
│   └── coordinator_track_detail_scrape.py
│
├── scrapers/                        # Logic gọi API và lưu dữ liệu
│   ├── artist_scraper.py
│   ├── album_scraper.py
│   ├── album_track_scraper.py
│   ├── album_detail_scraper.py
│   ├── genre_detail_scraper.py
│   ├── track_detail_scraper.py
│   ├── user_scraper.py
│   ├── user_fav_track_scraper.py
│   ├── user_fav_artist_scraper.py
│   └── user_fav_album_scraper.py
│
├── mapping_user_id/                 # Phân tích thống kê vùng ID
│   ├── mapper.py
│   └── coarse_scan.csv
│
└── superset/                        # Cấu hình Apache Superset
    ├── dockerfile
    └── entrypoint.sh
```

---

## 📊 Trực quan hóa với Apache Superset

Sau khi cào đủ dữ liệu, có thể tạo **SQL View** từ PostgreSQL rồi kết nối với Superset để trả lời các câu hỏi như:

- 🌍 Top 10 quốc gia có nhiều người dùng nhất
- 🎵 Nghệ sĩ nào có nhiều fan nhất?
- 📅 Xu hướng release album theo năm?
- 🎼 Thể loại nhạc nào được yêu thích nhất?

**Kết nối DBeaver với PostgreSQL container:**

| Trường | Giá trị |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `deezer` |
| User | `admin` |
| Password | `admin` |

---

## 🛠️ Yêu cầu

- Docker & Docker Compose
- (Tuỳ chọn) DBeaver để truy vấn trực tiếp PostgreSQL
