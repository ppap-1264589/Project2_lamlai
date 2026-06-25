import argparse
import os

from coordinators.coordinator_artist_scrape import run_artist_scrape
from coordinators.coordinator_user_scrape import run_user_scrape
from coordinators.coordinator_genre_detail_scrape import run_genre_detail_scrape
from coordinators.coordinator_track_detail_scrape import run_track_detail_scrape
from coordinators.coordinator_album_detail_scrape import run_album_detail_scrape

COORDINATORS = {
    "artist":       run_artist_scrape,
    "user":         run_user_scrape,
    "genre":        run_genre_detail_scrape,
    "track_detail": run_track_detail_scrape,
    "album_detail": run_album_detail_scrape,
}

def main():
    parser = argparse.ArgumentParser(description="Start a Deezer scrape coordinator.")
    parser.add_argument(
        "target",
        nargs="?",
        default=os.getenv("SCRAPER_TARGET", "artist"),
        help="Coordinator to run: artist | user | genre | track_detail",
    )
    args = parser.parse_args()
    target = args.target.lower()

    if target not in COORDINATORS:
        parser.error(f"Unknown target {target!r}. Choose: {', '.join(COORDINATORS)}")

    print(f"🚀 Starting [{target}] coordinator")
    COORDINATORS[target]()

if __name__ == "__main__":
    main()