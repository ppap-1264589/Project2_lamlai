"""
Debug script: đọc albums từ DB, gọi Deezer API, so sánh với scrape_status hiện tại.

Cách dùng:
    pip install psycopg2-binary aiohttp
    python debug_albums.py

Sửa phần CONFIG bên dưới cho khớp với docker-compose của bạn.
"""

import asyncio
import aiohttp
import psycopg2
import psycopg2.extras
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "deezer"
DB_USER     = "admin"
DB_PASSWORD = "admin"

ALBUM_URL   = "https://api.deezer.com/album/{id}"
HEADERS     = {"Accept-Language": "en-US"}

SAMPLE_SIZE = 20        # số album lấy ra để check
CONCURRENCY = 5         # request song song (nhỏ thôi để không bị rate limit)
# ─────────────────────────────────────────────────────────────


def get_sample_albums(statuses: list[str] | None = None) -> list[dict]:
    """Lấy mẫu album từ DB, mỗi status lấy đều nhau."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Lấy phân phối status hiện tại
    cur.execute("SELECT scrape_status, COUNT(*) FROM albums GROUP BY scrape_status ORDER BY scrape_status")
    print("\n── DB status distribution ──────────────────────────")
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row['scrape_status']:12s}  {row['count']:>10,}")
    print()

    # Lấy mẫu từ từng status
    if statuses is None:
        cur.execute("SELECT DISTINCT scrape_status FROM albums")
        statuses = [r["scrape_status"] for r in cur.fetchall()]

    per_status = max(1, SAMPLE_SIZE // len(statuses))
    albums = []
    for status in statuses:
        cur.execute("""
            SELECT id, scrape_status
            FROM albums
            WHERE scrape_status = %s
            ORDER BY RANDOM()
            LIMIT %s
        """, (status, per_status))
        albums.extend(cur.fetchall())

    cur.close()
    conn.close()
    return [dict(r) for r in albums]


async def check_album(session: aiohttp.ClientSession, sem: asyncio.Semaphore, album: dict) -> dict:
    album_id = album["id"]
    db_status = album["scrape_status"]

    async with sem:
        try:
            async with session.get(
                ALBUM_URL.format(id=album_id),
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=5, sock_read=10),
            ) as resp:
                raw_status = resp.status
                content_type = resp.headers.get("Content-Type", "")

                try:
                    data = await resp.json(content_type=None)
                    has_error_key = "error" in data
                    fields = ["title", "genre_id", "duration", "release_date", "record_type", "fans"]
                    has_data = any(data.get(f) not in (None, "") for f in fields)
                    body_summary = {
                        "has_error_key": has_error_key,
                        "has_data":      has_data,
                        "error_detail":  data.get("error") if has_error_key else None,
                        "title":         data.get("title"),
                    }
                except Exception as parse_err:
                    body_summary = {"parse_error": str(parse_err)}
                    has_data = False

                # Tính expected status theo logic hiện tại
                if raw_status == 404:
                    expected = "not_found"
                elif raw_status == 429:
                    expected = "rlimit"
                elif raw_status != 200:
                    expected = "error"
                elif body_summary.get("parse_error"):
                    expected = "error"
                elif body_summary.get("has_data"):
                    expected = "done"
                elif body_summary.get("has_error_key"):
                    expected = "error"
                else:
                    expected = "no_data"

                mismatch = (db_status != expected)

                return {
                    "id":            album_id,
                    "db_status":     db_status,
                    "http_status":   raw_status,
                    "content_type":  content_type,
                    "expected":      expected,
                    "mismatch":      mismatch,
                    "body":          body_summary,
                }

        except asyncio.TimeoutError:
            return {"id": album_id, "db_status": db_status, "error": "TimeoutError"}
        except Exception as e:
            return {"id": album_id, "db_status": db_status, "error": f"{type(e).__name__}: {e}"}


async def main():
    albums = get_sample_albums()
    print(f"Checking {len(albums)} albums from DB...\n")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = [check_album(session, sem, a) for a in albums]
        results = await asyncio.gather(*tasks)

    # ── Summary ───────────────────────────────────────────────
    mismatches = [r for r in results if r.get("mismatch")]
    errors     = [r for r in results if "error" in r and "mismatch" not in r]

    print(f"── Results ─────────────────────────────────────────")
    print(f"  Total checked : {len(results)}")
    print(f"  Mismatches    : {len(mismatches)}")
    print(f"  Fetch errors  : {len(errors)}")

    if mismatches:
        print(f"\n── Mismatches (db_status → expected) ───────────────")
        by_pair = defaultdict(list)
        for r in mismatches:
            key = f"{r['db_status']} → {r['expected']}"
            by_pair[key].append(r)
        for pair, items in by_pair.items():
            print(f"\n  [{pair}]  ({len(items)} albums)")
            for r in items[:5]:   # in tối đa 5 ví dụ mỗi loại
                print(f"    id={r['id']}  http={r['http_status']}  body={r['body']}")

    if errors:
        print(f"\n── Fetch errors ────────────────────────────────────")
        for r in errors[:10]:
            print(f"  id={r['id']}  {r['error']}")

    # In chi tiết từng result nếu muốn
    print(f"\n── Full detail ─────────────────────────────────────")
    for r in results:
        flag = "⚠ " if r.get("mismatch") else ("✗ " if "error" in r else "✓ ")
        print(f"  {flag} id={r.get('id')}  db={r.get('db_status')}  "
              f"http={r.get('http_status', 'N/A')}  expected={r.get('expected', 'N/A')}")


if __name__ == "__main__":
    asyncio.run(main())