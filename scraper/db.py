import psycopg2

SCHEMA_SETUP_LOCK_ID = 2026062401


def get_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="deezer",
        user="admin",
        password="admin",
    )

def setup_tables(conn):
    cur = conn.cursor()

    try:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_SETUP_LOCK_ID,))

        cur.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                id       BIGINT PRIMARY KEY,
                name     TEXT,
                nb_album INT,
                nb_fan   INT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id   INT PRIMARY KEY,
                name TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                id           BIGINT PRIMARY KEY,
                title        TEXT,
                genre_id     INT,
                release_date DATE,
                record_type  TEXT,
                fans         INT
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_albums_genre_id
            ON albums (genre_id)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS artist_albums (
                artist_id BIGINT REFERENCES artists(id),
                album_id  BIGINT REFERENCES albums(id),
                role      TEXT,
                PRIMARY KEY (artist_id, album_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id       BIGINT PRIMARY KEY,
                title    TEXT,
                duration INT,
                rank     INT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS album_tracks (
                album_id       BIGINT REFERENCES albums(id),
                track_id       BIGINT REFERENCES tracks(id),
                track_position INT,
                PRIMARY KEY (album_id, track_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id        BIGINT PRIMARY KEY,
                name      TEXT,
                lastname  TEXT,
                firstname TEXT,
                email     TEXT,
                birthday  DATE,
                gender    TEXT,
                country   TEXT,
                lang      TEXT,
                is_kid    BOOLEAN
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_fav_tracks (
                user_id  BIGINT REFERENCES users(id),
                track_id BIGINT REFERENCES tracks(id),
                time_add TIMESTAMP,
                PRIMARY KEY (user_id, track_id)
            )
        """)

        # Progress riêng cho từng luồng scraper
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scrape_progress (
                scraper TEXT PRIMARY KEY,
                last_id BIGINT NOT NULL
            )
        """)

        conn.commit()
        print("✅ Bảng đã sẵn sàng")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
