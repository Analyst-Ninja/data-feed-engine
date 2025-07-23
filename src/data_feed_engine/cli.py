import argparse
from datetime import datetime
from data_feed_engine.runner import run_feed_from_config
import logging


def main():
    parser = argparse.ArgumentParser(description="CLI for Data Feed Engine")

    parser.add_argument(
        "--config", "-c", required=True, help="Path to feed config JSON file"
    )
    parser.add_argument(
        "--date",
        "-d",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date in YYYY-MM-DD format",
    )
    parser.add_argument("--full-load", "-f", default=False, help="Perform a full load")

    args = parser.parse_args()

    run_feed_from_config(
        config_path=args.config, run_date=args.date, full_load=args.full_load
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
