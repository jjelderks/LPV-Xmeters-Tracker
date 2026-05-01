#!/usr/bin/env python3
"""
generate_billing.py — Generate quarterly billing invoice tabs.

Reads meter usage from the main water meters Google Sheet and writes
calculated values into per-lot invoice tabs in a new billing workbook.
Variable costs billed in quarter N come from quarter N-1.

Usage:
    python src/generate_billing.py --quarter 3 --year 2026
    python src/generate_billing.py --quarter 3 --year 2026 --dry-run
"""
import argparse
import os
import re
import time

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../config/.env"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAIN_SHEET_ID    = "1I14yVDrcpY6C2tABWDSxZ_MRjAyyFjjFKlrC2JBwh0g"   # LPV Water Meters
COSTS_SHEET_ID   = "1YHGambbpzGhSPttzOLpm04XKL4BN0GZhLTdw6VHFHcc"   # Water System Costs 2026
INVOICES_SHEET_ID = "10e8pIc32hJZzqMV7MlKDf8iKcMteqWpDu-wt_wyGrXA"  # LPV Q1-Q2 2026 Invoices (source)

METER_INSTALL_DATE = pd.Timestamp("2026-01-06")

# Tab base name → tracker meter names (combined for multi-meter lots)
LOT_METER_MAP = {
    "Lot1":           ["LPV Lot 01"],
    "Lot4":           ["LPV Lot 04"],
    "Lot5":           ["LPV Lot 05"],
    "Lot7":           ["LPV Lot 07"],
    "Lot8":           ["LPV Lot 08"],
    "Lot9":           ["LPV Lot 09"],
    "Lot14":          ["LPV Lot 14"],
    "Lot15":          ["LPV Lot 15"],
    "Lot22":          ["LPV Lot 22"],
    "Lot23":          ["LPV Lot 23"],
    "Lot24":          ["LPV Lot 24"],
    "Lot25":          ["LPV Lot 25"],
    "Lot26":          ["LPV Lot 26"],
    "LotS2":          ["S2 - Liron Casa"],
    "LotS3":          ["S3 - Liron rental"],
    "LotS9":          ["S9(a) - Oded duplex", "S9(b) - Oded duplex"],
    "Casita":         ["LPV_Casita"],
    "Lot11-13,16,17": [],   # no meters — 5 lots, fixed charges only
    "Lot18-21":       [],   # no meters — 4 lots, fixed charges only
    # LotS1 handled separately — has two distinct sub-meter invoice sections
}

# LotS1 sub-meters (each gets its own section in the tab)
LOT_S1_METER_A = "S1(a) - Amit Magden"
LOT_S1_METER_B = "S1(b) - Amit Magden"


# ── Date helpers ──────────────────────────────────────────────────────────────

def _quarter_dates(q, year):
    start_month = (q - 1) * 3 + 1
    q_start = pd.Timestamp(year, start_month, 1)
    q_end = (q_start + pd.offsets.QuarterEnd(0)).normalize()
    return q_start, q_end


def _prev_quarter(q, year):
    return (4, year - 1) if q == 1 else (q - 1, year)


def _prev_quarter_info(billing_q, billing_year):
    prev_q, prev_year = _prev_quarter(billing_q, billing_year)
    raw_start, prev_end = _quarter_dates(prev_q, prev_year)
    prev_start = METER_INSTALL_DATE if (prev_q == 1 and prev_year == 2026) else raw_start
    prev_days = (prev_end - prev_start).days + 1
    return prev_q, prev_year, prev_start, prev_end, prev_days


def _fmt_date(ts):
    """Format as '6-Jan-2026'"""
    return ts.strftime("%-d-%b-%Y")


def _fmt_billing_date(ts):
    """Format as 'Apr 1, 2026'"""
    return ts.strftime("%b %-d, %Y")


def _fmt_billing_period(start, end):
    """Format as 'April 1 - June 30, 2026'"""
    return f"{start.strftime('%B %-d')} - {end.strftime('%B %-d, %Y')}"


# ── gspread helpers ───────────────────────────────────────────────────────────

def _make_client():
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
    )
    return gspread.authorize(creds)


def _get_or_create_workbook(client, name):
    try:
        return client.open(name)
    except gspread.exceptions.SpreadsheetNotFound:
        wb = client.create(name)
        print(f"  Created new workbook: {name}")
        return wb


# ── Data loaders ──────────────────────────────────────────────────────────────

def load_data():
    print("Loading meter data from Google Sheets…")
    client = _make_client()
    ss = client.open_by_key(MAIN_SHEET_ID)
    summary_df = pd.DataFrame(ss.worksheet("Summary").get_all_records())
    daily_df = pd.DataFrame(ss.worksheet("Daily Readings").get_all_records())
    daily_df["Date"] = pd.to_datetime(daily_df["Date"], errors="coerce")
    daily_df = daily_df.dropna(subset=["Date"])
    daily_df["Daily Usage (m³)"] = pd.to_numeric(daily_df["Daily Usage (m³)"], errors="coerce")
    daily_df["Total Flow (m³)"] = pd.to_numeric(daily_df["Total Flow (m³)"], errors="coerce")

    # Build initial readings dict {name: float} from Summary sheet
    initial_readings = {}
    for _, row in summary_df.iterrows():
        name = str(row.get("Name", "")).strip()
        val  = row.get("Initial Reading (m³)", "")
        if name:
            try:
                initial_readings[name] = float(val) if str(val).strip() not in ("", "nan") else None
            except (ValueError, TypeError):
                pass

    return summary_df, daily_df, initial_readings


def load_variable_costs(prev_start, prev_end):
    print("Loading variable costs…")
    client = _make_client()
    ws = client.open_by_key(COSTS_SHEET_ID).worksheet("Variable Costs")
    rows = ws.get_all_values()
    total = 0.0
    for row in rows[1:]:
        if len(row) >= 5 and row[0].strip() and row[4].strip():
            try:
                date = pd.to_datetime(row[0].strip(), dayfirst=True)
                if prev_start <= date <= prev_end:
                    total += float(row[4].replace(",", "").replace("$", ""))
            except Exception:
                pass
    return total


# ── Meter data helpers ────────────────────────────────────────────────────────

def _get_reading(daily_df, meter_name, date):
    """Total flow reading for a meter on a specific date. Returns None if missing."""
    rows = daily_df[(daily_df["Name"] == meter_name) & (daily_df["Date"] == date)]
    if rows.empty:
        return None
    return float(rows.iloc[0]["Total Flow (m³)"])


def _get_usage(daily_df, meter_names, start, end):
    """Sum daily usage for meters within [start, end]."""
    mask = (
        daily_df["Name"].isin(meter_names) &
        (daily_df["Date"] > start) &  # usage period starts day after begin reading
        (daily_df["Date"] <= end)
    )
    return float(daily_df[mask]["Daily Usage (m³)"].sum())


def _get_total_system_usage(daily_df, all_meter_names, start, end):
    """Total usage across all metered lots in the period."""
    mask = (
        daily_df["Name"].isin(all_meter_names) &
        (daily_df["Date"] > start) &
        (daily_df["Date"] <= end)
    )
    return float(daily_df[mask]["Daily Usage (m³)"].sum())


def _get_total_system_usage_q1(daily_df, all_meter_names, initial_readings, end):
    """
    Q1-specific total usage: end flow minus initial reading for each meter.
    Used instead of summing daily_usage because daily data only starts Feb 25,
    so the daily sum would miss 7 weeks of Jan–Feb usage.
    """
    total = 0.0
    for name in all_meter_names:
        end_reading = _get_reading(daily_df, name, end)
        init = initial_readings.get(name)
        if end_reading is not None and init is not None:
            total += end_reading - init
    return total


def _get_usage_q1(daily_df, meter_names, initial_readings, end):
    """
    Q1-specific per-lot usage: end flow minus initial reading.
    Returns (usage, begin_total, end_total).
    """
    begin_total = sum(
        v for m in meter_names
        if (v := initial_readings.get(m)) is not None
    )
    end_total = sum(
        v for m in meter_names
        if (v := _get_reading(daily_df, m, end)) is not None
    )
    return end_total - begin_total, begin_total, end_total


def _find_date_col(ws_values, row_idx):
    """Detect whether dates in a row are in column D or E (returns 'D' or 'E')."""
    if row_idx >= len(ws_values):
        return "E"
    row = ws_values[row_idx]
    for col_idx, col_letter in [(3, "D"), (4, "E")]:
        if col_idx < len(row) and re.search(r"\d{1,2}-[A-Za-z]{3}-\d{4}", str(row[col_idx])):
            return col_letter
    return "E"


# ── Tab filling ───────────────────────────────────────────────────────────────

def _build_standard_updates(ws_values, meters, prev_q, prev_start, prev_end,
                             prev_days, prev_var_total, total_system_usage,
                             daily_df, billing_q, billing_year,
                             initial_readings=None):
    """
    Build cell updates for standard tabs (all lots except LotS1).
    Returns list of (cell_a1, value) tuples.
    """
    billing_q_start, billing_q_end = _quarter_dates(billing_q, billing_year)
    updates = []

    # Header
    updates += [
        ("B1",  f"💧 LPV Water System - Q{billing_q} {billing_year} Statement"),
        ("B6",  _fmt_billing_date(billing_q_start)),
        ("B7",  _fmt_billing_period(billing_q_start, billing_q_end)),
    ]

    # Fixed charges section label and clear pump surcharge row 19
    updates += [
        ("A15", f"Fixed Charges (Q{billing_q})"),
        ("B19", ""),
        ("C19", ""),
        ("D19", ""),
        ("E19", ""),
    ]

    # Variable charges section
    updates += [
        ("A22", f"Variable Charges (Q{billing_q})"),
        ("B24", f"Total Project Water Maintenance Costs (Q{prev_q})"),
        ("F24", f"${prev_var_total:,.2f}"),
    ]

    # Date column detection (some tabs use D26/D27, others use E26/E27)
    date_col = _find_date_col(ws_values, 25)  # row 26 = index 25
    updates += [
        (f"{date_col}26", _fmt_date(prev_start)),
        (f"{date_col}27", _fmt_date(prev_end)),
    ]

    # Readings and usage
    is_q1 = (prev_start == METER_INSTALL_DATE)
    if meters:
        if is_q1 and initial_readings:
            # Q1: daily data only starts Feb 25, so use end_flow − initial_reading
            usage, begin_total, end_total = _get_usage_q1(daily_df, meters, initial_readings, prev_end)
        else:
            # Q2+: full daily data available
            begin_date = prev_start - pd.Timedelta(days=1)
            begin_total = sum(
                v for m in meters
                if (v := _get_reading(daily_df, m, begin_date)) is not None
            )
            end_total = sum(
                v for m in meters
                if (v := _get_reading(daily_df, m, prev_end)) is not None
            )
            usage = _get_usage(daily_df, meters, prev_start, prev_end)
    else:
        begin_total = 0.0
        end_total = 0.0
        usage = 0.0

    updates += [
        ("F26", f"{begin_total:.4f}"),
        ("F27", f"{end_total:.4f}"),
        ("F28", f"{usage:.4f}"),
        ("F29", f"{total_system_usage:.4f}"),
    ]

    # Usage info section
    liters  = usage * 1000
    gallons = usage * 264.172
    avg_liters  = liters  / prev_days if prev_days > 0 else 0.0
    avg_gallons = gallons / prev_days if prev_days > 0 else 0.0
    updates += [
        ("A38", f"Q{prev_q} Usage Information"),
        ("F39", str(prev_days)),
        ("F40", f"{liters:,.2f}"),
        ("F41", f"{gallons:,.2f}"),
        ("F42", f"{avg_liters:,.2f}"),
        ("F43", f"{avg_gallons:,.2f}"),
    ]

    return updates


def _build_lots1_updates(prev_q, prev_start, prev_end, prev_days,
                         prev_var_total, total_system_usage,
                         daily_df, billing_q, billing_year,
                         initial_readings=None):
    """
    Build cell updates for the LotS1 tab (two separate sub-meter sections).
    """
    billing_q_start, billing_q_end = _quarter_dates(billing_q, billing_year)
    updates = []

    # Header
    updates += [
        ("B1", f"💧 LPV Water System - Q{billing_q} {billing_year} Statement"),
        ("B6", _fmt_billing_date(billing_q_start)),
        ("B7", _fmt_billing_period(billing_q_start, billing_q_end)),
    ]

    # Fixed charges (rows 13-18 for LotS1) — clear pump surcharge row 17
    updates += [
        ("A13", f"Fixed Charges (Q{billing_q})"),
        ("B17", ""),
        ("C17", ""),
        ("D17", ""),
        ("E17", ""),
    ]

    # Variable charges
    updates += [
        ("A20", f"Variable Charges (Q{billing_q})"),
        ("B22", f"Total Project Water Maintenance Costs (Q{prev_q})"),
        ("F22", f"${prev_var_total:,.2f}"),
    ]

    is_q1 = (prev_start == METER_INSTALL_DATE)

    if is_q1 and initial_readings:
        # Q1: use initial reading as begin, end_flow − initial as usage
        begin_a = initial_readings.get(LOT_S1_METER_A) or 0.0
        end_a   = _get_reading(daily_df, LOT_S1_METER_A, prev_end) or 0.0
        usage_a = end_a - begin_a
        begin_b = initial_readings.get(LOT_S1_METER_B) or 0.0
        end_b   = _get_reading(daily_df, LOT_S1_METER_B, prev_end) or 0.0
        usage_b = end_b - begin_b
    else:
        begin_date = prev_start - pd.Timedelta(days=1)
        begin_a = _get_reading(daily_df, LOT_S1_METER_A, begin_date) or 0.0
        end_a   = _get_reading(daily_df, LOT_S1_METER_A, prev_end)   or 0.0
        usage_a = _get_usage(daily_df, [LOT_S1_METER_A], prev_start, prev_end)
        begin_b = _get_reading(daily_df, LOT_S1_METER_B, begin_date) or 0.0
        end_b   = _get_reading(daily_df, LOT_S1_METER_B, prev_end)   or 0.0
        usage_b = _get_usage(daily_df, [LOT_S1_METER_B], prev_start, prev_end)

    updates += [
        ("D24", _fmt_date(prev_start)),
        ("F24", f"{begin_a:.4f}"),
        ("D25", _fmt_date(prev_end)),
        ("F25", f"{end_a:.4f}"),
        ("F26", f"{usage_a:.4f}"),
        ("F27", f"{total_system_usage:.4f}"),
    ]

    updates += [
        ("D32", _fmt_date(prev_start)),
        ("F32", f"{begin_b:.4f}"),
        ("D33", _fmt_date(prev_end)),
        ("F33", f"{end_b:.4f}"),
        ("F34", f"{usage_b:.4f}"),
        ("F35", f"{total_system_usage:.4f}"),
    ]

    # Combined usage info
    total_usage = usage_a + usage_b
    liters      = total_usage * 1000
    gallons     = total_usage * 264.172
    updates += [
        ("A41", f"Q{prev_q} Usage Information"),
        ("F42", str(prev_days)),
        ("F43", f"{total_usage:.4f}"),
        ("F44", f"{liters:,.2f}"),
        ("F45", f"{gallons:,.2f}"),
        ("F46", f"{total_usage / prev_days:.4f}" if prev_days > 0 else "0"),
        ("F47", f"{liters / prev_days:,.2f}"     if prev_days > 0 else "0"),
        ("F48", f"{gallons / prev_days:,.2f}"    if prev_days > 0 else "0"),
    ]

    return updates


# ── Tab creation and reordering ───────────────────────────────────────────────

def _q_suffix(q, year):
    return f"Q{q}{str(year)[-2:]}"


def _tab_base(title, q):
    """'Lot1-Q326' → 'Lot1'"""
    return re.sub(rf"-{_q_suffix(q, 2026)}$", "", title, flags=re.IGNORECASE)


def _copy_or_get_tab(src_wb, dst_wb, src_title, dst_title):
    """Copy a tab from src_wb to dst_wb with new title. Skip if already exists."""
    try:
        return dst_wb.worksheet(dst_title)
    except gspread.exceptions.WorksheetNotFound:
        pass
    src_ws = src_wb.worksheet(src_title)
    new_ws = src_ws.copy_to(dst_wb.id)
    new_ws.update_title(dst_title)
    time.sleep(1)
    return new_ws


def _reorder_tabs(wb):
    def _order(ws):
        t = ws.title.lower()
        m_s   = re.match(r"lots(\d+)", t)
        m_lot = re.match(r"lot(\d+)", t)
        if m_s:   return (1, int(m_s.group(1)), 0)
        if m_lot: return (0, int(m_lot.group(1)), 0)
        if "casita" in t: return (2, 0, 0)
        return (3, 0, 0)
    try:
        wb.reorder_worksheets(sorted(wb.worksheets(), key=_order))
    except Exception as e:
        print(f"  ⚠️  Tab reorder failed: {e}")


# ── Main generation ───────────────────────────────────────────────────────────

def generate(billing_q, billing_year, dry_run=False):
    prev_q, prev_year, prev_start, prev_end, prev_days = _prev_quarter_info(billing_q, billing_year)
    billing_q_start, billing_q_end = _quarter_dates(billing_q, billing_year)

    print(f"\nBilling period : Q{billing_q} {billing_year}  "
          f"({billing_q_start.date()} – {billing_q_end.date()})")
    print(f"Variable costs : Q{prev_q} {prev_year}  "
          f"({prev_start.date()} – {prev_end.date()}, {prev_days} days)")
    print(f"Dry run        : {dry_run}\n")

    summary_df, daily_df, initial_readings = load_data()
    prev_var_total = load_variable_costs(prev_start, prev_end)
    print(f"Q{prev_q} variable costs total: ${prev_var_total:,.2f}\n")

    all_metered = [m for meters in LOT_METER_MAP.values() for m in meters]
    all_metered += [LOT_S1_METER_A, LOT_S1_METER_B]

    is_q1 = (prev_start == METER_INSTALL_DATE)
    if is_q1:
        # Daily data only starts Feb 25; use end_flow − initial_reading for all meters
        total_system_usage = _get_total_system_usage_q1(daily_df, all_metered, initial_readings, prev_end)
        print(f"Q{prev_q} total system usage  : {total_system_usage:.4f} m³  (Q1 — end minus initial readings)\n")
    else:
        total_system_usage = _get_total_system_usage(daily_df, all_metered, prev_start, prev_end)
        print(f"Q{prev_q} total system usage  : {total_system_usage:.4f} m³\n")

    client  = _make_client()
    src_wb  = client.open_by_key(INVOICES_SHEET_ID)
    src_q   = prev_q  # Q2 tabs are the source for Q3
    src_sfx = _q_suffix(src_q, prev_year)
    dst_sfx = _q_suffix(billing_q, billing_year)
    dst_name = f"LPV Q{prev_q}-Q{billing_q} {billing_year} Invoices"

    if not dry_run:
        dst_wb = _get_or_create_workbook(client, dst_name)
        # Delete auto-created blank sheet if present
        sheets = dst_wb.worksheets()
        if len(sheets) == 1 and sheets[0].title in ("Sheet1", ""):
            pass  # will be replaced when we copy tabs

    results = []
    src_tabs = {ws.title: ws for ws in src_wb.worksheets()}

    for src_title, src_ws in src_tabs.items():
        base = _tab_base(src_title, src_q)
        dst_title = f"{base}-{dst_sfx}"

        # --- Get meter list ---
        if base == "LotS1":
            is_lots1 = True
            meters = [LOT_S1_METER_A, LOT_S1_METER_B]
        else:
            is_lots1 = False
            meters = LOT_METER_MAP.get(base)
            if meters is None:
                results.append(f"⚠️  {src_title} — base '{base}' not in LOT_METER_MAP, skipped")
                continue

        # --- Copy tab ---
        if not dry_run:
            dst_ws = _copy_or_get_tab(src_wb, dst_wb, src_title, dst_title)
            ws_values = dst_ws.get_all_values()
            time.sleep(1)
        else:
            ws_values = src_ws.get_all_values()

        # --- Build updates ---
        if is_lots1:
            updates = _build_lots1_updates(
                prev_q, prev_start, prev_end, prev_days,
                prev_var_total, total_system_usage,
                daily_df, billing_q, billing_year,
                initial_readings=initial_readings,
            )
        else:
            updates = _build_standard_updates(
                ws_values, meters, prev_q, prev_start, prev_end,
                prev_days, prev_var_total, total_system_usage,
                daily_df, billing_q, billing_year,
                initial_readings=initial_readings,
            )

        # --- Apply or preview ---
        if dry_run:
            print(f"\n{'─'*60}")
            print(f"  {dst_title}  ({len(meters)} meter(s))")
            for cell, val in updates:
                print(f"    {cell:>5}  ←  {repr(val)}")
        else:
            batch = [{"range": cell, "values": [[val]]} for cell, val in updates]
            dst_ws.batch_update(batch, value_input_option="USER_ENTERED")
            if meters:
                if is_q1:
                    usage, _, _ = _get_usage_q1(daily_df, meters, initial_readings, prev_end)
                else:
                    usage = _get_usage(daily_df, meters, prev_start, prev_end)
            else:
                usage = 0.0
            pct = usage / total_system_usage * 100 if total_system_usage > 0 else 0.0
            results.append(f"✅  {dst_title} — {len(updates)} cells, usage {usage:.3f} m³ ({pct:.2f}%)")
            time.sleep(3)

    if not dry_run:
        _reorder_tabs(dst_wb)
        print(f"\nWorkbook: {dst_name}\n")
        for r in results:
            print(r)

    print("\nDone.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate quarterly billing invoice tabs for LPV water meters."
    )
    parser.add_argument("--quarter", "-q", type=int, required=True, choices=[1, 2, 3, 4])
    parser.add_argument("--year",    "-y", type=int, default=pd.Timestamp.now().year)
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview all cell updates without writing anything")
    args = parser.parse_args()
    generate(args.quarter, args.year, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
