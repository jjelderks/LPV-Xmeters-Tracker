#!/usr/bin/env python3
"""
generate_billing.py — Generate quarterly billing invoice tabs.

Reads meter usage from the main water meters Google Sheet and writes
calculated values into per-lot invoice tabs in the billing workbook.

Billing quarter N uses variable costs from quarter N-1.
Special case: Q1 2026 starts Jan 6 (meter install date), not Jan 1.

Usage:
    python src/generate_billing.py --quarter 2 --year 2026
    python src/generate_billing.py --quarter 3 --year 2026
"""
import argparse
import os
import re
import sys
import time
import json

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../config/.env"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
MAIN_SHEET_ID      = "1I14yVDrcpY6C2tABWDSxZ_MRjAyyFjjFKlrC2JBwh0g"
BILLING_SHEET_ID   = "1YHGambbpzGhSPttzOLpm04XKL4BN0GZhLTdw6VHFHcc"
STATEMENTS_SHEET_ID = "1CPztsWoAWVOPjDpJZMKTSMLdPo4ie8V0-pF6gUGxS7o"
NON_BILLING_TABS   = {"Master Data", "Initial Reading", "Sheet1"}
METER_INSTALL_DATE = pd.Timestamp("2026-01-06")

# Month regex patterns per quarter (for replacing dates in invoice text)
_QUARTER_MONTH_RX = {
    1: r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?",
    2: r"Apr(?:il)?|May|Jun(?:e)?",
    3: r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?",
    4: r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?",
}


# ── Date helpers ──────────────────────────────────────────────────────────────

def _quarter_dates(q, year):
    """Return (start, end) Timestamps for a calendar quarter."""
    start_month = (q - 1) * 3 + 1
    q_start = pd.Timestamp(year, start_month, 1)
    q_end = (q_start + pd.offsets.QuarterEnd(0)).normalize()
    return q_start, q_end


def _prev_quarter(q, year):
    """Return (prev_q, prev_year)."""
    if q == 1:
        return 4, year - 1
    return q - 1, year


def _prev_quarter_info(billing_q, billing_year):
    """
    Return info about the variable cost (previous) quarter:
        prev_q, prev_year, prev_start, prev_end, prev_days
    Q1 2026 is special: starts from METER_INSTALL_DATE, not Jan 1.
    """
    prev_q, prev_year = _prev_quarter(billing_q, billing_year)
    raw_start, prev_end = _quarter_dates(prev_q, prev_year)

    # First operational quarter: start from install date
    if prev_q == 1 and prev_year == 2026:
        prev_start = METER_INSTALL_DATE
    else:
        prev_start = raw_start

    prev_days = (prev_end - prev_start).days + 1
    return prev_q, prev_year, prev_start, prev_end, prev_days


# ── gspread helpers ───────────────────────────────────────────────────────────

def _make_client():
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
    )
    return gspread.authorize(creds)


def _get_or_create_billing_workbook(client, prev_q, billing_q):
    name = f"Q{prev_q}-Q{billing_q} water billing"
    try:
        return client.open(name)
    except gspread.exceptions.SpreadsheetNotFound:
        sa_email = ""
        try:
            with open(os.environ["GOOGLE_CREDENTIALS_FILE"]) as f:
                sa_email = json.load(f).get("client_email", "")
        except Exception:
            pass
        raise RuntimeError(
            f"Spreadsheet '{name}' not found. "
            f"Create a blank Google Sheet named exactly '{name}' "
            f"in your Google Drive and share it (Editor) with: {sa_email}"
        )


# ── Data loaders ──────────────────────────────────────────────────────────────

def load_data():
    print("Loading meter data from Google Sheets…")
    client = _make_client()
    spreadsheet = client.open_by_key(MAIN_SHEET_ID)

    summary_ws = spreadsheet.worksheet("Summary")
    summary_df = pd.DataFrame(summary_ws.get_all_records())

    daily_ws = spreadsheet.worksheet("Daily Readings")
    daily_df = pd.DataFrame(daily_ws.get_all_records())
    daily_df["Date"] = pd.to_datetime(daily_df["Date"], errors="coerce")
    daily_df = daily_df.dropna(subset=["Date"])
    daily_df["Daily Usage (m³)"] = pd.to_numeric(daily_df["Daily Usage (m³)"], errors="coerce")
    daily_df["Total Flow (m³)"] = pd.to_numeric(daily_df["Total Flow (m³)"], errors="coerce")

    return summary_df, daily_df


def load_variable_costs():
    print("Loading variable costs…")
    client = _make_client()
    billing_sheet = client.open_by_key(BILLING_SHEET_ID)
    ws = billing_sheet.worksheet("Variable Costs")
    rows = ws.get_all_values()
    records = []
    for row in rows[1:]:
        if len(row) >= 5 and row[0].strip() and row[4].strip():
            try:
                date = pd.to_datetime(row[0].strip(), dayfirst=True)
                cost = float(row[4].replace(",", "").replace("$", ""))
                records.append({"Date": date, "Cost": cost})
            except Exception:
                pass
    return pd.DataFrame(records) if records else pd.DataFrame(columns=["Date", "Cost"])


# ── Usage calculation ─────────────────────────────────────────────────────────

def _get_prev_quarter_usage(daily_df, summary_df, prev_start, prev_end, is_install_quarter):
    """
    Return Series of usage (m³) per meter name for the previous (variable cost) quarter.
    For the install quarter, uses Initial Reading as the starting point.
    """
    end_readings = (
        daily_df[daily_df["Date"] == prev_end]
        .set_index("Name")["Total Flow (m³)"]
    )
    if is_install_quarter:
        begin_readings = (
            summary_df.set_index("Name")["Initial Reading (m³)"]
            .apply(pd.to_numeric, errors="coerce")
        )
    else:
        prev_prev_end = prev_start - pd.Timedelta(days=1)
        begin_readings = (
            daily_df[daily_df["Date"] == prev_prev_end]
            .set_index("Name")["Total Flow (m³)"]
        )
    return (end_readings - begin_readings).dropna().clip(lower=0)


# ── Main generation logic ─────────────────────────────────────────────────────

def generate_billing_tabs(daily_df, summary_df, variable_costs_df, billing_q, billing_year):
    """
    Generate billing invoice tabs for billing_q / billing_year.
    Variable costs billed are from the previous quarter.
    """
    prev_q, prev_year, prev_start, prev_end, prev_days = _prev_quarter_info(billing_q, billing_year)
    billing_q_start, billing_q_end = _quarter_dates(billing_q, billing_year)
    is_install_quarter = (prev_q == 1 and prev_year == 2026)

    # Date strings for text replacement
    prev_long_range    = f"{prev_start.strftime('%B %-d')} - {prev_end.strftime('%B %-d, %Y')}"
    prev_short_range   = f"{prev_start.strftime('%b. %-d')} - {prev_end.strftime('%b. %-d, %Y')}"
    billing_long_range = f"{billing_q_start.strftime('%B %-d')} - {billing_q_end.strftime('%B %-d, %Y')}"
    billing_short_range = f"{billing_q_start.strftime('%b. %-d')} - {billing_q_end.strftime('%b. %-d, %Y')}"
    billing_start_str  = billing_q_start.strftime('%B %-d, %Y')
    prev_start_cell    = prev_start.strftime("%-d-%b-%Y")   # e.g., "6-Jan-2026"
    prev_end_cell      = prev_end.strftime("%-d-%b-%Y")      # e.g., "31-Mar-2026"
    prev_month_rx      = _QUARTER_MONTH_RX[prev_q]

    # Usage and cost totals for the previous quarter
    prev_usage = _get_prev_quarter_usage(
        daily_df, summary_df, prev_start, prev_end, is_install_quarter
    )
    total_prev_usage = prev_usage.sum()

    prev_var_total = (
        variable_costs_df[
            (variable_costs_df["Date"] >= prev_start) &
            (variable_costs_df["Date"] <= prev_end)
        ]["Cost"].sum()
        if not variable_costs_df.empty else 0.0
    )

    # Readings at boundary dates
    prev_end_readings = (
        daily_df[daily_df["Date"] == prev_end]
        .set_index("Name")["Total Flow (m³)"]
    )
    billing_begin_readings = (
        daily_df[daily_df["Date"] == billing_q_start]
        .set_index("Name")["Total Flow (m³)"]
    )

    # Short meter number → tracker name lookup
    meter_to_name = {
        str(row["Meter Number"]).lstrip("0"): row["Name"]
        for _, row in summary_df.iterrows()
        if str(row.get("Meter Number", "")).strip()
    }

    client      = _make_client()
    statements  = client.open_by_key(STATEMENTS_SHEET_ID)
    billing_wb  = _get_or_create_billing_workbook(client, prev_q, billing_q)
    results     = []

    # Pre-load all source (prev quarter) tabs from the statements workbook
    prev_tabs = {}
    for ws in statements.worksheets():
        if ws.title in NON_BILLING_TABS:
            continue
        if not re.search(rf'q{prev_q}', ws.title, re.IGNORECASE):
            continue
        vals = ws.get_all_values()
        time.sleep(1)
        prev_tabs[ws.title.lower().replace(" ", "")] = (ws, vals)

    print(f"Found {len(prev_tabs)} Q{prev_q} source tabs.")

    # Process each billing quarter tab in the billing workbook
    for billing_ws in billing_wb.worksheets():
        title = billing_ws.title

        # Strip "Copy of " prefix from manually copied tabs
        if title.lower().startswith("copy of "):
            title = title[len("copy of "):]
            billing_ws.update_title(title)
            time.sleep(0.5)

        # Find the matching source (prev quarter) tab
        prev_title_guess = re.sub(rf'(?i)q{billing_q}', f'Q{prev_q}', title, count=1)
        prev_key = prev_title_guess.lower().replace(" ", "")

        prev_entry = prev_tabs.get(prev_key)
        if prev_entry is None:
            for k, (v_ws, v_vals) in prev_tabs.items():
                derived = re.sub(rf'(?i)q{prev_q}', f'q{billing_q}', v_ws.title, count=1).lower().replace(" ", "")
                if derived == title.lower().replace(" ", ""):
                    prev_entry = (v_ws, v_vals)
                    break

        if prev_entry is None:
            results.append(f"⚠️  {title} — no matching Q{prev_q} source tab found, skipped")
            continue

        _, source_values = prev_entry
        if not source_values:
            continue

        # Identify tracker meters in this tab by serial number
        tab_meter_names = set()
        for row in source_values:
            for cell in row:
                short = str(cell).strip().lstrip("0")
                if short in meter_to_name:
                    tab_meter_names.add(meter_to_name[short])

        # Fallback: match by initial reading value
        if not tab_meter_names:
            _init_map = {
                round(pd.to_numeric(r["Initial Reading (m³)"], errors="coerce"), 4): r["Name"]
                for _, r in summary_df.iterrows()
                if str(r.get("Initial Reading (m³)", "")).strip()
            }
            for _row in source_values:
                _row_str = " ".join(str(c) for c in _row).lower()
                if "beginning qtr reading" in _row_str:
                    for _cell in _row:
                        try:
                            _val = round(float(str(_cell).strip()), 4)
                            if _val in _init_map:
                                tab_meter_names.add(_init_map[_val])
                        except (ValueError, TypeError):
                            pass

        # Per-tab usage statistics
        tab_usage    = sum(float(prev_usage.get(n, 0)) for n in tab_meter_names)
        tab_pct      = round(tab_usage / total_prev_usage * 100 if total_prev_usage > 0 else 0.0, 2)
        tab_var_cost = round(tab_usage / total_prev_usage * prev_var_total if total_prev_usage > 0 else 0.0, 2)
        tab_liters   = round(tab_usage * 1000, 1)
        tab_gallons  = round(tab_usage * 264.172, 1)
        tab_avg_m3   = round(tab_usage / prev_days, 3)
        tab_avg_liters  = round(tab_liters / prev_days, 1)
        tab_avg_gallons = round(tab_gallons / prev_days, 1)

        initial_series = (
            summary_df.set_index("Name")["Initial Reading (m³)"]
            .apply(pd.to_numeric, errors="coerce")
        )
        tab_initial = sum(
            float(initial_series[n]) for n in tab_meter_names
            if n in initial_series.index and not pd.isna(initial_series[n])
        )

        try:
            billing_vals = billing_ws.get_all_values()
            updates = []
            a1 = gspread.utils.rowcol_to_a1

            def _val_cell(cs):
                s = cs.strip()
                if s in ("", "NA", "N/A", "0.00%", "$0.00", "0", "$ -", "$-"):
                    return True
                return not re.search(r'[a-zA-Z]', s)

            def _rightmost_val(row, row_i):
                for j in range(len(row) - 1, -1, -1):
                    if _val_cell(row[j]):
                        return a1(row_i + 1, j + 1)
                return None

            # Fix #REF! cells from broken cross-workbook formulas
            for i, row in enumerate(billing_vals):
                for j, cell in enumerate(row):
                    if str(cell).strip() == "#REF!":
                        src_val = source_values[i][j] if i < len(source_values) and j < len(source_values[i]) else ""
                        updates.append({"range": a1(i + 1, j + 1), "values": [[src_val]]})
                        billing_vals[i][j] = src_val

            # Fixed cell addresses
            updates.extend([
                {"range": "F16", "values": [["Costs"]]},
                {"range": "F20", "values": [["=SUM(F17:F19)"]]},
                {"range": "F24", "values": [[f"${prev_var_total:,.2f}"]]},
                {"range": "D26", "values": [[prev_start_cell]]},
                {"range": "F31", "values": [["=F30*F24"]]},
            ])

            # Update label in row 24
            if len(billing_vals) > 23:
                for j, cell in enumerate(billing_vals[23]):
                    if re.search(r'(?i)water\s+maintenance', str(cell)):
                        updates.append({
                            "range": a1(24, j + 1),
                            "values": [[f"Total Project Water Maintenance Costs (Q{prev_q})"]],
                        })
                        break

            # Row scanning
            for i, row in enumerate(billing_vals):
                rl = " ".join(row).lower().replace('\xa0', ' ').replace('\u00b3', '3')

                # Text replacements
                for j, cell in enumerate(row):
                    s = str(cell)
                    orig = s
                    s = re.sub(rf'\bQ{prev_q}\b', f'Q{billing_q}', s)
                    s = s.replace(prev_long_range,  billing_long_range)
                    s = s.replace(prev_short_range, billing_short_range)
                    s = re.sub(rf'(?:{prev_month_rx})\s+\d{{1,2}},?\s+{prev_year}', billing_start_str, s)
                    s = re.sub(r'(?i)since\s+meter\s+installation\s+date',
                               f"{prev_long_range}", s)
                    s = re.sub(r'(?i)see\s+hoa\s+invoice', 'Upon receipt, see invoice', s)
                    s = re.sub(r'(?i)payment\s+due\s+date\s*:.*',
                               'Payment due date: Upon receipt, see invoice', s)
                    s = re.sub(r'(?i)note\s*:\s*usage.based charges begin.*', '', s)
                    if re.search(r'(?i)^fixed\s+charges\s*$', s.strip()):
                        s = f'Fixed Charges (Q{billing_q})'
                    if re.search(r'(?i)^variable\s+charges\s*$', s.strip()):
                        s = f'Variable Charges (Q{prev_q})'
                    if s != orig:
                        updates.append({"range": a1(i + 1, j + 1), "values": [[s]]})

                # Beginning QTR Reading
                if "beginning qtr reading" in rl or "beginning quarter reading" in rl:
                    if tab_initial > 0:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[f"{tab_initial:.4f}"]]})
                    updates.append({"range": a1(i + 1, 4), "values": [[prev_start_cell]]})
                    continue

                # End QTR Reading
                if "end qtr reading" in rl or "end quarter reading" in rl:
                    end_total = sum(
                        float(prev_end_readings[n]) for n in tab_meter_names
                        if n in prev_end_readings.index
                    )
                    end_val = f"{end_total:.4f}" if end_total > 0 else None
                    if end_val:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[end_val]]})
                    updates.append({"range": a1(i + 1, 4), "values": [[prev_end_cell]]})
                    continue

                # Total Due / Total Charges
                if "total due" in rl or "total charges" in rl:
                    updates.append({"range": a1(i + 1, 6), "values": [["=F31+F20"]]})
                    continue

                # Subtotal rows (skip — F31 handles it)
                if "subtotal" in rl and "total charges" not in rl and "total water" not in rl:
                    continue

                # Other value fills
                ROW_VALUES = [
                    ("usage total" in rl and ("m3" in rl or "m³" in rl),                                    f"{tab_usage:.4f}"),
                    ("project total" in rl,                                                                  f"{total_prev_usage:.4f}"),
                    ("% of usage" in rl,                                                                     f"{tab_pct:.2f}%"),
                    ("total water system maintenance" in rl,                                                 f"${prev_var_total:,.2f}"),
                    ("number of days" in rl,                                                                 str(prev_days)),
                    (("total liters" in rl or ("liters" in rl and "total" in rl)) and "average" not in rl,  f"{tab_liters:,.1f}"),
                    (("total gallons" in rl or ("gallons" in rl and "total" in rl)) and "average" not in rl, f"{tab_gallons:,.1f}"),
                    ("average daily" in rl and "m³" in rl,                                                  f"{tab_avg_m3:.3f}"),
                    ("average daily" in rl and "liter" in rl,                                               f"{tab_avg_liters:,.1f}"),
                    ("average daily" in rl and "gallon" in rl,                                              f"{tab_avg_gallons:,.1f}"),
                ]
                for matches, new_val in ROW_VALUES:
                    if matches:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[new_val]]})
                        break

            if updates:
                billing_ws.batch_update(updates, value_input_option="USER_ENTERED")

            results.append(
                f"✅  {title} — {len(updates)} cells updated, "
                f"usage: {tab_usage:.3f} m³ ({tab_pct:.2f}%)"
            )

        except Exception as e:
            results.append(f"❌  {title}: {e}")

        time.sleep(4)  # stay under Sheets API quota

    # Reorder tabs: Lot1, Lot3…Lot26, then LotS1…LotS9, then Casita, then other
    def _tab_order(ws):
        t = ws.title.lower()
        m_s   = re.match(r'lots(\d+)', t)
        m_lot = re.match(r'lot(\d+)', t)
        if m_s:
            return (1, int(m_s.group(1)), 0)
        if m_lot:
            return (0, int(m_lot.group(1)), 0)
        if 'casita' in t:
            return (2, 0, 0)
        return (3, 0, 0)

    try:
        ordered = sorted(billing_wb.worksheets(), key=_tab_order)
        billing_wb.reorder_worksheets(ordered)
    except Exception as e:
        results.append(f"⚠️  Tab reorder failed: {e}")

    return results, prev_var_total


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate quarterly billing invoice tabs for LPV water meters."
    )
    parser.add_argument(
        "--quarter", "-q",
        type=int,
        required=True,
        choices=[1, 2, 3, 4],
        help="Billing quarter number (1–4)",
    )
    parser.add_argument(
        "--year", "-y",
        type=int,
        default=pd.Timestamp.now().year,
        help="Billing year (default: current year)",
    )
    args = parser.parse_args()

    prev_q, prev_year, prev_start, prev_end, prev_days = _prev_quarter_info(args.quarter, args.year)
    billing_q_start, billing_q_end = _quarter_dates(args.quarter, args.year)

    print(f"\nGenerating Q{args.quarter} {args.year} billing tabs")
    print(f"  Billing period : {billing_q_start.date()} – {billing_q_end.date()}")
    print(f"  Variable costs : Q{prev_q} {prev_year} ({prev_start.date()} – {prev_end.date()}, {prev_days} days)")
    print(f"  Workbook       : Q{prev_q}-Q{args.quarter} water billing\n")

    summary_df, daily_df = load_data()
    variable_costs_df    = load_variable_costs()

    print("\nGenerating billing tabs…\n")
    results, var_total = generate_billing_tabs(
        daily_df, summary_df, variable_costs_df, args.quarter, args.year
    )

    print(f"\nQ{prev_q} variable costs total: ${var_total:,.2f}\n")
    for r in results:
        print(r)
    print("\nDone.")


if __name__ == "__main__":
    main()
