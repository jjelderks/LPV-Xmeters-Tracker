"""
Scraper module - pulls meter data from xmeters.com REST API.
"""
import logging
import time
import requests
from datetime import datetime, date

logger = logging.getLogger(__name__)

BASE_URL = "https://xmeters.com"
ORG_CODE = "1992894424495214594"
TIMEZONE_OFFSET = "-05:00"

# Initial readings from January 6th (fetched via browser on 2026-03-16)
INITIAL_READINGS_JAN6 = {
    "LPV Lot 01":          19.6297,
    "LPV Lot 04":          34.3142,
    "LPV Lot 05":          29.4009,
    "LPV Lot 07":          14.9625,
    "LPV Lot 08":          37.4405,
    "LPV Lot 09":           6.0815,
    "LPV Lot 14":          23.3791,
    "LPV Lot 15":          47.8384,
    "LPV Lot 22":          23.5722,
    "LPV Lot 23":           3.9446,
    "LPV Lot 24":          94.2641,
    "LPV Lot 25":           5.3242,
    "LPV Lot 26":           8.4518,
    "LPV_Casita":          15.5487,
    "Los_Tanques":          0.4368,
    "S1(a) - Amit Magden": 44.4193,
    "S1(b) - Amit Magden": 43.9134,
    "S2 - Liron Casa":     24.3924,
    "S3 - Liron rental":   29.3847,
    "S9(a) - Oded duplex": 24.1759,
    "S9(b) - Oded duplex":  3.7941,
}

METERS = [
    ("LPV Lot 01",          "00000024150436"),
    ("LPV Lot 04",          "00000024150444"),
    ("LPV Lot 05",          "00000024150439"),
    ("LPV Lot 07",          "00000024150453"),
    ("LPV Lot 08",          "00000024150455"),
    ("LPV Lot 09",          "00000024150438"),
    ("LPV Lot 14",          "00000024150446"),
    ("LPV Lot 15",          "00000024150459"),
    ("LPV Lot 22",          "00000024150432"),
    ("LPV Lot 23",          "00000024150448"),
    ("LPV Lot 24",          "00000024150440"),
    ("LPV Lot 25",          "00000024150442"),
    ("LPV Lot 26",          "00000024150458"),
    ("LPV_Casita",          "00000024150441"),
    ("Los_Tanques",         "00000024150454"),
    ("S1(a) - Amit Magden", "00000024150431"),
    ("S1(b) - Amit Magden", "00000024150451"),
    ("S2 - Liron Casa",     "00000024150434"),
    ("S3 - Liron rental",   "00000024150457"),
    ("S9(a) - Oded duplex", "00000024150435"),
    ("S9(b) - Oded duplex", "00000024150445"),
]



class MeterScraper:
    def __init__(self, username: str, password: str):
        self.session = requests.Session()
        self._login(username, password)

    def _login(self, username: str, password: str):
        resp = self.session.post(f"{BASE_URL}/water/sys/login", json={
            "username": username,
            "password": password,
            "isCaptcha": 0,
            "captcha": "",
            "checkKey": None,
        })
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(f"Login failed: {data.get('message')}")
        token = data["result"]["token"]
        self.session.headers.update({
            "X-Access-Token": token,
            "X-Tenant-Id": "0",
            "X-Version": "v3",
        })
        logger.info("Logged in to xmeters.com")

    def get_initial_readings(self) -> dict:
        """Return Jan 6 initial readings (hardcoded from site, fetched 2026-03-16)."""
        return {
            name: {"meter_number": meter_id, "initial_reading": INITIAL_READINGS_JAN6.get(name)}
            for name, meter_id in METERS
        }

    def get_daily_readings(self, start_date: str, end_date: str) -> list[dict]:
        """
        Fetch daily total_flow readings for all meters between start_date and end_date.
        start_date / end_date format: "YYYY-MM-DD"
        Returns list of dicts with keys: name, meter_number, date, total_flow, daily_usage
        """
        start_dt = f"{start_date} 00:00:00"
        end_dt = f"{end_date} 23:59:59"
        all_rows = []

        for name, meter_id in METERS:
            logger.info(f"Fetching {name} ({meter_id})...")
            try:
                resp = self.session.post(
                    f"{BASE_URL}/water/visualization/meterData",
                    json={
                        "meterId": meter_id,
                        "offset": TIMEZONE_OFFSET,
                        "startTime": start_dt,
                        "endTime": end_dt,
                        "groupHours": 24,
                        "protocol": "1",
                        "meterOrgCode": ORG_CODE,
                        "relativeOn": "1",
                        "columns": "total_flow",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                if not data.get("success"):
                    logger.warning(f"  API error for {name}: {data.get('message')}")
                    continue

                for row in data.get("result", []):
                    if row.get("totalFlow") is None:
                        continue
                    all_rows.append({
                        "name": name,
                        "meter_number": meter_id,
                        "date": row["xtime"][:10],
                        "total_flow": float(row["totalFlow"]),
                        "daily_usage": float(row.get("totalFlowDiff") or 0),
                    })

                time.sleep(0.3)  # be polite to the server

            except Exception as e:
                logger.error(f"  Error fetching {name}: {e}")

        return all_rows
