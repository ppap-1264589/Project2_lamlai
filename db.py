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
                id                  INT PRIMARY KEY,
                name                TEXT,
                scrape_status       TEXT NOT NULL DEFAULT 'pending',
                scrape_attempted_at TIMESTAMPTZ
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_genres_pending
            ON genres (id)
            WHERE scrape_status = 'pending'
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                id                  BIGINT PRIMARY KEY,
                title               TEXT,
                genre_id            INT,
                duration            INT,
                release_date        DATE,
                record_type         TEXT,
                fans                INT,
                scrape_status       TEXT NOT NULL DEFAULT 'pending',
                scrape_attempted_at TIMESTAMPTZ
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_albums_genre_id
            ON albums (genre_id)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_albums_pending
            ON albums (id)
            WHERE scrape_status = 'pending'
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
                id                  BIGINT PRIMARY KEY,
                title               TEXT,
                duration            INT,
                rank                INT,
                bpm                 REAL,
                release_date        DATE,
                scrape_status       TEXT NOT NULL DEFAULT 'pending',
                scrape_attempted_at TIMESTAMPTZ
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_pending
            ON tracks (id)
            WHERE scrape_status = 'pending'
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
            CREATE TABLE IF NOT EXISTS track_countries (
                track_id BIGINT PRIMARY KEY REFERENCES tracks(id),
                countries TEXT[]
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_track_countries_gin
            ON track_countries USING GIN (countries)
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

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_fav_artists (
                user_id   BIGINT REFERENCES users(id),
                artist_id BIGINT REFERENCES artists(id),
                time_add  TIMESTAMP,
                PRIMARY KEY (user_id, artist_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_fav_albums (
                user_id  BIGINT REFERENCES users(id),
                album_id BIGINT REFERENCES albums(id),
                time_add TIMESTAMP,
                PRIMARY KEY (user_id, album_id)
            )
        """)

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