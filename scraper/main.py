import argparse
import os

from coordinator_artist_scrape import run_artist_scrape
from coordinator_user_scrape import run_user_scrape


COORDINATORS = {
    "artist": run_artist_scrape,
    "user": run_user_scrape,
}


def main():
    parser = argparse.ArgumentParser(description="Start a Deezer scrape coordinator.")
    parser.add_argument(
        "target",
        nargs="?",
        default=os.getenv("SCRAPER_TARGET", "artist"),
        help="Coordinator to run: artist or user. Defaults to SCRAPER_TARGET, then artist.",
    )
    args = parser.parse_args()

    target = args.target.lower()
    if target not in COORDINATORS:
        parser.error(f"unknown target {args.target!r}; choose one of: {', '.join(COORDINATORS)}")

    print(f"Starting {target} scrape coordinator")
    COORDINATORS[target]()


if __name__ == "__main__":
    main()
