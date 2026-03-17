"""
Google Sheets integration - writes meter readings to a spreadsheet.
"""
import gspread
from google.oauth2.service_account import Credentials
import logging

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsWriter:
    def __init__(self, credentials_file: str, sheet_id: str):
        creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_key(sheet_id)

    def _get_or_create_worksheet(self, title: str, rows: int = 2000, cols: int = 30) -> gspread.Worksheet:
        try:
            return self.spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = self.spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
            logger.info(f"Created worksheet: {title}")
            return ws

    def write_daily_readings(self, readings: list[dict]):
        """
        Write all daily readings to the 'Daily Readings' tab.
        Clears and rewrites the full sheet each run.
        Columns: Name | Meter Number | Date | Total Flow (m³) | Daily Usage (m³)
        """
        ws = self._get_or_create_worksheet("Daily Readings")
        ws.clear()

        headers = ["Name", "Meter Number", "Date", "Total Flow (m³)", "Daily Usage (m³)"]
        rows = [headers] + [
            [r["name"], r["meter_number"], r["date"], r["total_flow"], r["daily_usage"]]
            for r in sorted(readings, key=lambda x: (x["name"], x["date"]))
        ]
        ws.update("A1", rows)
        logger.info(f"Written {len(rows)-1} rows to 'Daily Readings'.")

    def write_summary(self, readings: list[dict], initials: dict):
        """
        Write a summary tab with one row per meter:
        Name | Meter Number | Initial Reading (Jan 6, 2026) | Latest Total Flow | Total Usage Since Feb 28
        """
        ws = self._get_or_create_worksheet("Summary")
        ws.clear()

        # Get latest reading per meter
        latest = {}
        for r in readings:
            name = r["name"]
            if name not in latest or r["date"] > latest[name]["date"]:
                latest[name] = r

        # Get total usage per meter (sum of daily_usage from Feb 28)
        total_usage = {}
        for r in readings:
            name = r["name"]
            total_usage[name] = total_usage.get(name, 0) + r["daily_usage"]

        headers = [
            "Name", "Meter Number",
            "Initial Reading (m³)",
            "Initial Reading Date",
            "Latest Total Flow (m³)",
            "Total Usage Since Initial Reading (m³)",
            "Last Updated",
        ]

        rows = [headers]
        all_names = sorted(set([r["name"] for r in readings] + list(initials.keys())))
        for name in all_names:
            init = initials.get(name, {})
            lat = latest.get(name, {})
            initial_val = init.get("initial_reading")
            initial_date = init.get("initial_reading_date", "")
            latest_flow = lat.get("total_flow")
            if initial_val is not None and latest_flow is not None:
                total_usage = round(float(latest_flow) - float(initial_val), 4)
            else:
                total_usage = ""
            rows.append([
                name,
                init.get("meter_number") or lat.get("meter_number", ""),
                initial_val if initial_val is not None else "",
                initial_date,
                latest_flow if latest_flow is not None else "",
                total_usage,
                lat.get("date", ""),
            ])

        # Convert None to "" for clean sheet output
        clean_rows = [
            [("" if v is None else v) for v in row]
            for row in rows
        ]
        ws.update("A1", clean_rows)
        logger.info(f"Written {len(rows)-1} rows to 'Summary'.")

    def log_spike(self, spike: dict):
        """
        Append a spike event to the 'Spike Log' tab.
        Only adds new rows — never clears existing ones so notes are preserved.
        spike keys: date, meter, usage, normal_avg, threshold
        """
        HEADERS = ["Date", "Meter", "Usage (m³)", "Normal Avg (m³)",
                   "Threshold (m³)", "Alerted", "Reason", "Resolved"]

        ws = self._get_or_create_worksheet("Spike Log", rows=1000, cols=10)
        all_rows = ws.get_all_values()

        # Write header if first row doesn't match expected headers
        if not all_rows or all_rows[0] != HEADERS:
            ws.clear()
            ws.append_row(HEADERS)
            all_rows = [HEADERS]

        # Skip if this spike is already logged (same date + meter)
        for row in all_rows[1:]:
            if len(row) >= 2 and row[0] == spike["date"] and row[1] == spike["meter"]:
                logger.info(f"Spike already logged for {spike['meter']} on {spike['date']}, skipping.")
                return

        ws.append_row([
            spike["date"],
            spike["meter"],
            round(spike["usage"], 4),
            round(spike["normal_avg"], 4),
            round(spike["threshold"], 4),
            "Yes",
            "",
            "No",
        ], value_input_option="RAW")
        logger.info(f"Spike logged: {spike['meter']} on {spike['date']}")
