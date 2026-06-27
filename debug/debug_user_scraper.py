"""
Debug script: xem raw response từ Deezer user API.

Chạy: docker compose run --rm --no-deps scraper_user python debug/debug_user_scraper.py

Không cần DB. Chỉ cần container và internet/VPN.
"""

import requests
import time
import json
import sys
import os

# Cho phép import config.py từ thư mục gốc khi chạy file này trong debug/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import USER_URL, HEADERS

# ── CONFIG ────────────────────────────────────────────────────
USER_ID_START       = 19092        # user_id bắt đầu
USER_ID_END         = 19223       # user_id kết thúc (inclusive)
REQUESTS_PER_SECOND = 20        # số request mỗi giây
TIMEOUT             = 5        # timeout mỗi request (giây)
PRINT_FULL_RESPONSE = True     # True = in toàn bộ JSON trả về
PRINT_ONLY_ERRORS   = False    # True = chỉ in các ID bị lỗi
STOP_ON_ERROR       = False    # True = dừng ngay khi gặp lỗi đầu tiên
RATE_LIMIT_DELAY    = 1 / REQUESTS_PER_SECOND
# ─────────────────────────────────────────────────────────────


def fetch_user_debug(user_id: int) -> dict:
    url = USER_URL.format(id=user_id)
    result = {
        "user_id":     user_id,
        "url":         url,
        "http_status": None,
        "error":       None,
        "raw":         None,
        "parsed":      None,
    }
    try:
        resp = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        result["http_status"] = resp.status_code
        result["raw"] = resp.text[:500] if len(resp.text) > 500 else resp.text

        data = resp.json()
        result["raw_json"] = data

        if "error" in data:
            result["error"] = f"API error: {data['error']}"
        else:
            result["parsed"] = {
                "id":        data.get("id"),
                "name":      data.get("name"),
                "lastname":  data.get("lastname"),
                "firstname": data.get("firstname"),
                "email":     data.get("email"),
                "birthday":  data.get("birthday"),
                "gender":    data.get("gender"),
                "country":   data.get("country"),
                "lang":      data.get("lang"),
                "is_kid":    data.get("is_kid"),
            }

    except requests.exceptions.Timeout:
        result["error"] = "TIMEOUT"
    except requests.exceptions.ConnectionError as e:
        result["error"] = f"CONNECTION ERROR: {e}"
    except requests.exceptions.JSONDecodeError as e:
        result["error"] = f"JSON DECODE ERROR: {e} | raw: {result['raw']}"
    except Exception as e:
        result["error"] = f"UNEXPECTED: {type(e).__name__}: {e}"

    return result


def print_result(r: dict):
    sep = "─" * 60
    is_error = r["error"] is not None

    if PRINT_ONLY_ERRORS and not is_error:
        return

    tag = "✅" if not is_error else "❌"
    print(f"\n{sep}")
    print(f"{tag} user_id={r['user_id']}  HTTP={r['http_status']}  url={r['url']}")

    if is_error:
        print(f"   ERROR: {r['error']}")

    if PRINT_FULL_RESPONSE and r.get("raw_json"):
        print("   RAW JSON:")
        print(json.dumps(r["raw_json"], indent=4, ensure_ascii=False)[:1000])
    elif r.get("parsed"):
        print("   PARSED:")
        for k, v in r["parsed"].items():
            print(f"     {k:12s} = {v!r}")


def main():
    print("=" * 60)
    print(f"Debug user scraper")
    print(f"  Range      : {USER_ID_START} → {USER_ID_END}")
    print(f"  Delay      : {RATE_LIMIT_DELAY}s")
    print(f"  Timeout    : {TIMEOUT}s")
    print(f"  Full JSON  : {PRINT_FULL_RESPONSE}")
    print(f"  Errors only: {PRINT_ONLY_ERRORS}")
    print("=" * 60)

    stats = {"total": 0, "found": 0, "not_found": 0, "error": 0}

    for user_id in range(USER_ID_START, USER_ID_END + 1):
        result = fetch_user_debug(user_id)
        print_result(result)
        stats["total"] += 1

        if result["error"]:
            if "API error" in str(result["error"]):
                stats["not_found"] += 1
            else:
                stats["error"] += 1
            if STOP_ON_ERROR:
                print("\n⛔ STOP_ON_ERROR=True, dừng lại.")
                break
        else:
            stats["found"] += 1

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n{'=' * 60}")
    print(f"Kết quả: total={stats['total']} | found={stats['found']} | not_found={stats['not_found']} | error={stats['error']}")
    print("=" * 60)


if __name__ == "__main__":
    main()