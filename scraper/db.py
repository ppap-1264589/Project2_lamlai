import psycopg2

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id       BIGINT PRIMARY KEY,
            name     TEXT,
            nb_album INT,
            nb_fan   INT
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
        CREATE TABLE IF NOT EXISTS scrape_progress (
            last_id BIGINT NOT NULL
        )
    """)

    conn.commit()
    cur.close()
    print("✅ Bảng đã sẵn sàng")