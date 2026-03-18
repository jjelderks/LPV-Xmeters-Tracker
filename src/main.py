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

# Track usage from Feb 28 onward
USAGE_START_DATE = "2026-02-25"


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

    writer = SheetsWriter(
        credentials_file=os.environ["GOOGLE_CREDENTIALS_FILE"],
        sheet_id=os.environ["GOOGLE_SHEET_ID"],
    )

    # Read user-set thresholds before rewriting summary
    min_thresholds = writer.get_min_thresholds()
    max_thresholds = writer.get_max_thresholds()
    logger.info(f"Loaded min/max thresholds for {len(min_thresholds)}/{len(max_thresholds)} meters.")

    # Write daily readings tab
    writer.write_daily_readings(readings)
    writer.write_summary(readings, initials)

    # Check alerts and send WhatsApp notifications
    logger.info("Checking alerts...")
    check_alerts(readings, sheets_writer=writer, min_thresholds=min_thresholds, max_thresholds=max_thresholds)

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
