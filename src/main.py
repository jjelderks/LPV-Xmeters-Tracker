"""
LPV-Xmeters-Tracker - main entry point.
Fetches meter readings and writes them to Google Sheets.
"""
import os
import logging
import schedule
import time
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

from scraper import MeterScraper
from sheets import SheetsWriter
from notify import check_alerts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "../logs/tracker.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

USAGE_START_DATE = "2026-02-25"
LAST_CHECK_FILE  = os.path.join(os.path.dirname(__file__), "../data/last_spike_check.txt")


def _get_last_checked_date():
    try:
        with open(LAST_CHECK_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def _set_last_checked_date(date_str):
    with open(LAST_CHECK_FILE, "w") as f:
        f.write(date_str)


def run():
    logger.info("Starting meter data fetch...")

    scraper = MeterScraper(
        username=os.environ["SITE_USERNAME"],
        password=os.environ["SITE_PASSWORD"],
    )

    end_date = date.today().strftime("%Y-%m-%d")

    # Daily + total usage from Feb 28
    logger.info(f"Fetching daily readings {USAGE_START_DATE} → {end_date}")
    readings = scraper.get_daily_readings(USAGE_START_DATE, end_date)
    logger.info(f"Fetched {len(readings)} daily reading rows.")

    # Initial readings (Jan 6)
    logger.info("Fetching initial readings (Jan 6)...")
    initials = scraper.get_initial_readings()
    logger.info(f"Fetched initial readings for {len(initials)} meters.")

    _creds_raw = os.environ["GOOGLE_CREDENTIALS_FILE"]
    _creds_file = _creds_raw if os.path.isabs(_creds_raw) else os.path.join(os.path.dirname(__file__), "..", _creds_raw)
    writer = SheetsWriter(
        credentials_file=os.path.abspath(_creds_file),
        sheet_id=os.environ["GOOGLE_SHEET_ID"],
    )

    # Read user-set thresholds before rewriting summary
    min_thresholds = writer.get_min_thresholds()
    max_thresholds = writer.get_max_thresholds()
    logger.info(f"Loaded min/max thresholds for {len(min_thresholds)}/{len(max_thresholds)} meters.")

    # Write daily readings tab
    writer.write_daily_readings(readings)
    writer.write_summary(readings, initials)

    # Determine which dates need spike checking (backfill any missed days)
    all_dates = sorted({r["date"] for r in readings})
    last_checked = _get_last_checked_date()
    if last_checked:
        check_dates = [d for d in all_dates if d > last_checked]
    else:
        check_dates = [all_dates[-1]] if all_dates else []

    if len(check_dates) > 1:
        logger.info(f"Backfilling spike check for {len(check_dates)} dates ({check_dates[0]} → {check_dates[-1]})...")
    else:
        logger.info("Checking alerts...")

    check_alerts(readings, check_dates=check_dates, sheets_writer=writer,
                 min_thresholds=min_thresholds, max_thresholds=max_thresholds)

    if all_dates:
        _set_last_checked_date(all_dates[-1])

    logger.info("Done.")


if __name__ == "__main__":
    import sys

    if "--once" in sys.argv:
        run()
    else:
        schedule_time = os.environ.get("SCHEDULE_TIME", "06:00")
        schedule.every().day.at(schedule_time).do(run)
        logger.info(f"Scheduler started. Running daily at {schedule_time}.")
        while True:
            schedule.run_pending()
            time.sleep(60)
