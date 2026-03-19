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

    def _read_summary_column(self, col_name: str) -> dict:
        """Read a named column from the Summary sheet. Returns {name: float}."""
        try:
            ws = self.spreadsheet.worksheet("Summary")
            rows = ws.get_all_values()
            if not rows:
                return {}
            headers = rows[0]
            if col_name not in headers:
                return {}
            name_idx = headers.index("Name")
            col_idx = headers.index(col_name)
            result = {}
            for row in rows[1:]:
                if len(row) > max(name_idx, col_idx):
                    name = row[name_idx]
                    val = row[col_idx]
                    try:
                        result[name] = float(val) if val != "" else 0.0
                    except ValueError:
                        result[name] = 0.0
            return result
        except Exception:
            return {}

    def get_min_thresholds(self) -> dict:
        return self._read_summary_column("Min Alert (m³)")

    def get_max_thresholds(self) -> dict:
        return self._read_summary_column("Max Daily (m³)")

    def write_summary(self, readings: list[dict], initials: dict):
        """
        Write a summary tab with one row per meter.
        Preserves Bedrooms and Min Alert values set by the user.
        Max Daily is a formula driven by the Bedrooms column.
        Columns: A=Name B=Meter C=Initial D=InitDate E=LatestFlow F=TotalUsage G=LastUpdated H=MinAlert I=Bedrooms J=MaxDaily
        """
        ws = self._get_or_create_worksheet("Summary")

        # Preserve user-set values before clearing
        existing_min = self.get_min_thresholds()
        existing_bedrooms = self._read_summary_column("Bedrooms")

        ws.clear()

        # Get latest reading per meter
        latest = {}
        for r in readings:
            name = r["name"]
            if name not in latest or r["date"] > latest[name]["date"]:
                latest[name] = r

        # Get total usage per meter
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
            "Min Alert (m³)",
            "Bedrooms",
            "Max Daily (m³)",
        ]

        rows = [headers]
        all_names = sorted(set([r["name"] for r in readings] + list(initials.keys())))
        for i, name in enumerate(all_names):
            row_num = i + 2  # Row 1 is header, data starts at row 2
            init = initials.get(name, {})
            lat = latest.get(name, {})
            initial_val = init.get("initial_reading")
            initial_date = init.get("initial_reading_date", "")
            latest_flow = lat.get("total_flow")
            if initial_val is not None and latest_flow is not None:
                usage_since = round(float(latest_flow) - float(initial_val), 4)
            else:
                usage_since = ""
            min_alert = existing_min.get(name, "")
            bedrooms = existing_bedrooms.get(name, "")
            # Formula: Studio(0)=1.0, 1bed=1.5, 2bed=2.5, 3bed=4.0, 4+bed=5.0
            max_formula = (
                f'=IF(I{row_num}="","",IF(I{row_num}=0,1.0,IF(I{row_num}=1,1.5,'
                f'IF(I{row_num}=2,2.5,IF(I{row_num}=3,4.0,5.0)))))'
            )
            rows.append([
                name,
                init.get("meter_number") or lat.get("meter_number", ""),
                initial_val if initial_val is not None else "",
                initial_date,
                latest_flow if latest_flow is not None else "",
                usage_since,
                lat.get("date", ""),
                min_alert,
                bedrooms,
                max_formula,
            ])

        # Use USER_ENTERED so formulas in Max Daily column are evaluated
        clean_rows = [
            [("" if v is None else v) for v in row]
            for row in rows
        ]
        ws.update("A1", clean_rows, value_input_option="USER_ENTERED")
        logger.info(f"Written {len(rows)-1} rows to 'Summary'.")

    def log_spike(self, spike: dict):
        """
        Append a spike event to the 'Spike Log' tab.
        Only adds new rows — never clears existing ones so notes are preserved.
        spike keys: date, meter, usage, normal_avg, threshold
        """
        HEADERS = ["Date", "Meter", "Usage (m³)", "Normal Avg (m³)",
                   "Threshold (m³)", "Min Alert (m³)", "Alerted", "Reason", "Resolved"]

        ws = self._get_or_create_worksheet("Spike Log", rows=1000, cols=10)
        all_rows = ws.get_all_values()

        # If sheet is empty, write headers
        if not all_rows:
            ws.append_row(HEADERS, value_input_option="RAW")
            all_rows = [HEADERS]
        # If headers are outdated, add any missing columns without clearing data
        elif all_rows[0] != HEADERS:
            existing_headers = all_rows[0]
            missing = [h for h in HEADERS if h not in existing_headers]
            if missing:
                # Append missing headers to the right of existing ones
                new_header_row = existing_headers + missing
                ws.update("A1", [new_header_row], value_input_option="RAW")
                all_rows[0] = new_header_row
                logger.info(f"Added missing Spike Log columns: {missing}")

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
            round(spike.get("min_alert", 0), 4) if spike.get("min_alert") else "",
            "Yes",
            "",
            "No",
        ], value_input_option="RAW")
        logger.info(f"Spike logged: {spike['meter']} on {spike['date']}")
