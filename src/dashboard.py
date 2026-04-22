"""
LPV Water Meters - Streamlit Dashboard
Run: streamlit run src/dashboard.py
"""
import os
import sys
import base64
import time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../config/.env"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
BILLING_SHEET_ID    = "1YHGambbpzGhSPttzOLpm04XKL4BN0GZhLTdw6VHFHcc"
STATEMENTS_SHEET_ID = "1CPztsWoAWVOPjDpJZMKTSMLdPo4ie8V0-pF6gUGxS7o"
NON_BILLING_TABS    = {"Master Data", "Initial Reading", "Sheet1"}

st.set_page_config(
    page_title="LPV Water Meters",
    page_icon="💧",
    layout="wide",
)

st.markdown("""
    <style>
    [data-baseweb="tag"] {
        background-color: #4C7FAF !important;
    }
    </style>
""", unsafe_allow_html=True)

def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("💧 LPV Water Meters")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        valid_user = st.secrets.get("USERNAME", os.environ.get("DASH_USERNAME", "LPV_medidores"))
        valid_pass = st.secrets.get("PASSWORD", os.environ.get("DASH_PASSWORD", "agua"))
        if username == valid_user and password == valid_pass:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid username or password")
    return False

if not check_password():
    st.stop()

def _get_gspread_client():
    if "GOOGLE_CREDENTIALS_JSON" in st.secrets:
        import json
        info = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        sheet_id = st.secrets["GOOGLE_SHEET_ID"]
    else:
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
        )
        sheet_id = os.environ["GOOGLE_SHEET_ID"]
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

@st.cache_data(ttl=3600)
def load_data():
    spreadsheet = _get_gspread_client()
    summary_ws = spreadsheet.worksheet("Summary")
    summary_df = pd.DataFrame(summary_ws.get_all_records())
    daily_ws = spreadsheet.worksheet("Daily Readings")
    daily_df = pd.DataFrame(daily_ws.get_all_records())
    daily_df["Date"] = pd.to_datetime(daily_df["Date"], errors="coerce")
    daily_df = daily_df.dropna(subset=["Date"])
    daily_df["Daily Usage (m³)"] = pd.to_numeric(daily_df["Daily Usage (m³)"], errors="coerce")
    daily_df["Total Flow (m³)"] = pd.to_numeric(daily_df["Total Flow (m³)"], errors="coerce")
    return summary_df, daily_df

@st.cache_data(ttl=3600)
def load_variable_costs():
    empty = pd.DataFrame(columns=["Date", "Cost"])
    try:
        if "GOOGLE_CREDENTIALS_JSON" in st.secrets:
            import json
            info = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"])
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(
                os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
            )
        client = gspread.authorize(creds)
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
                except (ValueError, Exception):
                    pass
        return pd.DataFrame(records) if records else empty, None
    except Exception as e:
        return empty, str(e)

def _make_gspread_client():
    if "GOOGLE_CREDENTIALS_JSON" in st.secrets:
        import json
        info = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"])
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(
            os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
        )
    return gspread.authorize(creds)

def _get_statements_spreadsheet():
    return _make_gspread_client().open_by_key(STATEMENTS_SHEET_ID)

def _get_or_create_q2_workbook(client):
    try:
        return client.open("Q1-Q2 water billing")
    except gspread.exceptions.SpreadsheetNotFound:
        # Get service account email to show in the error message
        sa_email = ""
        try:
            if "GOOGLE_CREDENTIALS_JSON" in st.secrets:
                import json
                sa_email = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"]).get("client_email", "")
            else:
                import json
                with open(os.environ["GOOGLE_CREDENTIALS_FILE"]) as f:
                    sa_email = json.load(f).get("client_email", "")
        except Exception:
            pass
        raise RuntimeError(
            f"Spreadsheet 'Q1-Q2 water billing' not found. "
            f"Please create a blank Google Sheet named exactly 'Q1-Q2 water billing' "
            f"in your Google Drive and share it (Editor) with: {sa_email}"
        )


def _get_q1_usage_per_meter(daily_df, summary_df):
    """Return Series of Q1 usage (m³) per meter name: Total Flow Mar 31 – Initial Reading Jan 6."""
    q1_end = pd.Timestamp("2026-03-31")
    mar31 = (
        daily_df[daily_df["Date"] == q1_end]
        .set_index("Name")["Total Flow (m³)"]
    )
    initial = (
        summary_df.set_index("Name")["Initial Reading (m³)"]
        .apply(pd.to_numeric, errors="coerce")
    )
    return (mar31 - initial).dropna().clip(lower=0)


def generate_q2_billing_tabs(daily_df, summary_df, variable_costs_df):
    import re

    q1_usage      = _get_q1_usage_per_meter(daily_df, summary_df)
    total_q1_usage = q1_usage.sum()
    q1_days        = 84  # Jan 6 → Mar 31

    # Q1 end readings (Mar 31) and Q2 begin readings (Apr 1)
    _q1_end_dt = pd.Timestamp("2026-03-31")
    _q2_begin_dt = pd.Timestamp("2026-04-01")
    q1_end_readings = (
        daily_df[daily_df["Date"] == _q1_end_dt]
        .set_index("Name")["Total Flow (m³)"]
    )
    q2_begin_readings = (
        daily_df[daily_df["Date"] == _q2_begin_dt]
        .set_index("Name")["Total Flow (m³)"]
    )

    q1_var_total = (
        variable_costs_df[
            (variable_costs_df["Date"] >= pd.Timestamp("2026-01-01")) &
            (variable_costs_df["Date"] <= pd.Timestamp("2026-03-31"))
        ]["Cost"].sum()
        if not variable_costs_df.empty else 0.0
    )

    # Short meter number (no leading zeros) → tracker name
    meter_to_name = {
        str(row["Meter Number"]).lstrip("0"): row["Name"]
        for _, row in summary_df.iterrows()
        if str(row.get("Meter Number", "")).strip()
    }

    client     = _make_gspread_client()
    spreadsheet = client.open_by_key(STATEMENTS_SHEET_ID)
    q2_book    = _get_or_create_q2_workbook(client)
    results    = []

    # Build a lookup of Q1 tabs + pre-load all values to avoid reads inside the main loop
    q1_tabs = {}
    for ws in spreadsheet.worksheets():
        if ws.title in NON_BILLING_TABS:
            continue
        if not re.search(r'q1', ws.title, re.IGNORECASE):
            continue
        vals = ws.get_all_values()
        time.sleep(1)
        q1_tabs[ws.title.lower().replace(" ", "")] = (ws, vals)

    # Iterate over tabs that actually exist in the Q1-Q2 workbook
    for q2_ws in q2_book.worksheets():
        title = q2_ws.title
        # Strip "Copy of " prefix added by Google Sheets manual copy
        if title.lower().startswith("copy of "):
            title = title[len("copy of "):]
            q2_ws.update_title(title)
            time.sleep(0.5)
        # Derive the expected Q1 tab name from this Q2 tab name
        q1_title_guess = re.sub(r'(?i)q2', 'Q1', title, count=1)
        q1_key = q1_title_guess.lower().replace(" ", "")

        # Try exact match first, then case-insensitive
        q1_entry = q1_tabs.get(q1_key)
        if q1_entry is None:
            for k, (v_ws, v_vals) in q1_tabs.items():
                derived = re.sub(r'(?i)q1', 'q2', v_ws.title, count=1).lower().replace(" ", "")
                if derived == title.lower().replace(" ", ""):
                    q1_entry = (v_ws, v_vals)
                    break

        if q1_entry is None:
            results.append(f"⚠️ {title} — no matching Q1 tab found in statements, skipped")
            continue

        ws, values = q1_entry
        if not values:
            continue

        # Identify tracker meters in this tab by serial number
        tab_meter_names = set()
        for row in values:
            for cell in row:
                short = str(cell).strip().lstrip("0")
                if short in meter_to_name:
                    tab_meter_names.add(meter_to_name[short])

        # Fallback: match by initial reading value (handles tabs where Meter ID shows "-")
        if not tab_meter_names:
            _init_map = {
                round(pd.to_numeric(r["Initial Reading (m³)"], errors="coerce"), 4): r["Name"]
                for _, r in summary_df.iterrows()
                if str(r.get("Initial Reading (m³)", "")).strip()
            }
            for _row in values:
                _row_str = " ".join(str(c) for c in _row).lower()
                if "beginning qtr reading" in _row_str:
                    for _cell in _row:
                        try:
                            _val = round(float(str(_cell).strip()), 4)
                            if _val in _init_map:
                                tab_meter_names.add(_init_map[_val])
                        except (ValueError, TypeError):
                            pass

        primary_meter = next(iter(tab_meter_names), None)

        tab_q1_usage    = sum(float(q1_usage.get(n, 0)) for n in tab_meter_names)
        tab_pct         = round(tab_q1_usage / total_q1_usage * 100 if total_q1_usage > 0 else 0.0, 2)
        tab_var_cost    = round(tab_q1_usage / total_q1_usage * q1_var_total if total_q1_usage > 0 else 0.0, 2)
        tab_liters      = round(tab_q1_usage * 1000, 1)
        tab_gallons     = round(tab_q1_usage * 264.172, 1)
        tab_avg_m3      = round(tab_q1_usage / q1_days, 3)
        tab_avg_liters  = round(tab_liters / q1_days, 1)
        tab_avg_gallons = round(tab_gallons / q1_days, 1)

        try:
            q2_vals = q2_ws.get_all_values()
            updates  = []

            a1 = gspread.utils.rowcol_to_a1

            def _val_cell(cs):
                s = cs.strip()
                if s in ("", "NA", "N/A", "0.00%", "$0.00", "0", "$ -", "$-"):
                    return True
                return not re.search(r'[a-zA-Z]', s)

            def _rightmost_val(row, row_i):
                for j2 in range(len(row) - 1, -1, -1):
                    if _val_cell(row[j2]):
                        return a1(row_i + 1, j2 + 1)
                return None

            # Initial reading for this tab (sum for multi-meter tabs)
            initial_series = (
                summary_df.set_index("Name")["Initial Reading (m³)"]
                .apply(pd.to_numeric, errors="coerce")
            )
            tab_initial = sum(
                float(initial_series[n]) for n in tab_meter_names
                if n in initial_series.index and not pd.isna(initial_series[n])
            )

            # ── Fix #REF! cells from broken cross-workbook formulas ────────
            for i, row in enumerate(q2_vals):
                for j, cell in enumerate(row):
                    if str(cell).strip() == "#REF!":
                        q1_val = values[i][j] if i < len(values) and j < len(values[i]) else ""
                        updates.append({"range": a1(i + 1, j + 1), "values": [[q1_val]]})
                        q2_vals[i][j] = q1_val

            # ── Known cell addresses (user-specified) ──────────────────────
            updates.extend([
                {"range": "F16", "values": [["Costs"]]},
                {"range": "F20", "values": [["=SUM(F17:F19)"]]},
                {"range": "F24", "values": [[f"${q1_var_total:,.2f}"]]},
                {"range": "D26", "values": [["6-Jan-2026"]]},
                {"range": "F31", "values": [["=F30*F24"]]},
            ])
            # Update label in row 24
            if len(q2_vals) > 23:
                for j, cell in enumerate(q2_vals[23]):
                    if re.search(r'(?i)water\s+maintenance', str(cell)):
                        updates.append({"range": a1(24, j + 1),
                                        "values": [["Total Project Water Maintenance Costs (Q1)"]]})
                        break

            # ── Row scanning ───────────────────────────────────────────────
            in_variable_section = False
            for i, row in enumerate(q2_vals):
                rl = " ".join(row).lower()

                if "variable charges" in rl:
                    in_variable_section = True

                # Text replacements
                for j, cell in enumerate(row):
                    s = str(cell)
                    orig = s
                    s = re.sub(r'\bQ1\b', 'Q2', s)
                    s = s.replace("January 1 - March 31, 2026", "April 1 - June 30, 2026")
                    s = s.replace("Jan. 1 - Mar. 31, 2026", "Apr. 1 - Jun. 30, 2026")
                    s = re.sub(r'(Jan(?:uary)?|Feb(?:ruary)?)\s+\d{1,2},?\s+2026', 'April 1, 2026', s)
                    s = re.sub(r'(?i)since\s+meter\s+installation\s+date', 'January 6 \u2013 March 31, 2026', s)
                    s = re.sub(r'(?i)see\s+hoa\s+invoice', 'Upon receipt, see invoice', s)
                    s = re.sub(r'(?i)payment\s+due\s+date\s*:.*', 'Payment due date: Upon receipt, see invoice', s)
                    s = re.sub(r'(?i)note\s*:\s*usage.based charges begin.*', '', s)
                    if re.search(r'(?i)^fixed\s+charges\s*$', s.strip()):
                        s = 'Fixed Charges (Q2)'
                    if re.search(r'(?i)^variable\s+charges\s*$', s.strip()):
                        s = 'Variable Charges (Q1)'
                    if s != orig:
                        updates.append({"range": a1(i + 1, j + 1), "values": [[s]]})

                # Beginning QTR Reading — value + date
                if "beginning qtr reading" in rl or "beginning quarter reading" in rl:
                    if tab_initial > 0:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[f"{tab_initial:.4f}"]]})
                    updates.append({"range": a1(i + 1, 4), "values": [["6-Jan-2026"]]})  # col D
                    continue

                # End QTR Reading — value + date (sum all meters in this tab)
                if "end qtr reading" in rl or "end quarter reading" in rl:
                    end_total = sum(
                        float(q1_end_readings[n]) for n in tab_meter_names
                        if n in q1_end_readings.index
                    )
                    end_val = f"{end_total:.4f}" if end_total > 0 else None
                    if end_val:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[end_val]]})
                    updates.append({"range": a1(i + 1, 4), "values": [["31-Mar-2026"]]})  # col D
                    continue

                # Total charges row — formula
                if "total due" in rl or "total charges" in rl:
                    updates.append({"range": a1(i + 1, 6), "values": [["=F31+F20"]]})  # col F
                    continue

                # Variable subtotal row (in variable section only — F31 handles it, skip)
                if "subtotal" in rl and "total charges" not in rl and "total water" not in rl:
                    continue

                # Other value fills
                ROW_VALUES = [
                    ("usage total" in rl and "m³" in rl,                                         f"{tab_q1_usage:.4f}"),
                    ("project total" in rl,                                                       f"{total_q1_usage:.4f}"),
                    ("% of usage" in rl,                                                          f"{tab_pct:.2f}%"),
                    ("total water system maintenance" in rl,                                      f"${q1_var_total:,.2f}"),
                    ("number of days" in rl,                                                      str(q1_days)),
                    (("total liters" in rl or ("liters" in rl and "total" in rl)) and "average" not in rl,  f"{tab_liters:,.1f}"),
                    (("total gallons" in rl or ("gallons" in rl and "total" in rl)) and "average" not in rl, f"{tab_gallons:,.1f}"),
                    ("average daily" in rl and "m³" in rl,                                       f"{tab_avg_m3:.3f}"),
                    ("average daily" in rl and "liter" in rl,                                    f"{tab_avg_liters:,.1f}"),
                    ("average daily" in rl and "gallon" in rl,                                   f"{tab_avg_gallons:,.1f}"),
                ]
                for (matches, new_val) in ROW_VALUES:
                    if matches:
                        cell_a1 = _rightmost_val(row, i)
                        if cell_a1:
                            updates.append({"range": cell_a1, "values": [[new_val]]})
                        break

            if updates:
                q2_ws.batch_update(updates, value_input_option="USER_ENTERED")

            results.append(
                f"✅ {title} — usage: {tab_q1_usage:.3f} m³ ({tab_pct:.2f}% of project)"
            )

        except Exception as e:
            results.append(f"❌ {title}: {e}")

        time.sleep(4)  # stay under Sheets API quota (60 reads/min)

    # Reorder tabs: Lot1, Lot3...Lot26, then LotS1...LotS9, then Casita, then anything else
    def _tab_order(ws):
        t = ws.title.lower()
        m_s  = re.match(r'lots(\d+)', t)
        m_lot = re.match(r'lot(\d+)', t)
        if m_s:
            return (1, int(m_s.group(1)), 0)
        if m_lot:
            return (0, int(m_lot.group(1)), 0)
        if 'casita' in t:
            return (2, 0, 0)
        return (3, 0, 0)

    try:
        ordered = sorted(q2_book.worksheets(), key=_tab_order)
        q2_book.reorder_worksheets(ordered)
    except Exception as e:
        results.append(f"⚠️ Tab reorder failed: {e}")

    return results, q1_var_total


@st.cache_data(ttl=3600)
def load_spike_log():
    try:
        spreadsheet = _get_gspread_client()
        spike_ws = spreadsheet.worksheet("Spike Log")
        rows = spike_ws.get_all_values()
        if len(rows) > 1:
            return pd.DataFrame(rows[1:], columns=rows[0])
    except Exception:
        pass
    return pd.DataFrame()

# --- Load ---
with st.spinner("Loading data..."):
    summary_df, daily_df = load_data()
    spike_df = load_spike_log()
    variable_costs_df, variable_costs_error = load_variable_costs()

# --- Header ---
st.markdown("<h1 style='text-align:center;'>💧 LPV Water Meter Dashboard</h1>", unsafe_allow_html=True)
st.markdown(f"<p style='text-align:center; color:gray;'>Data from Feb 25, 2026 · Updates nightly · Last meter date: {daily_df['Date'].max().strftime('%Y-%m-%d')}</p>", unsafe_allow_html=True)

st.divider()

# --- Shared computed values (needed by both tabs) ---
usage_col = next((c for c in summary_df.columns if "Total Usage" in c), None)
if usage_col is None:
    st.error("Could not find 'Total Usage Since' column in Summary sheet.")
    st.stop()

def clean_avg(values):
    import statistics
    non_zero = [v for v in values if v > 0]
    if not non_zero:
        return 0
    median = statistics.median(non_zero)
    filtered = [v for v in non_zero if v <= median * 2.5]
    return sum(filtered) / len(filtered) if filtered else sum(non_zero) / len(non_zero)

SPIKE_DISPLAY_FROM = pd.Timestamp("2026-03-15")
all_meters = sorted(daily_df["Name"].unique())

def _thresh_map(df, col):
    if col not in df.columns:
        return {}
    return {
        row["Name"]: float(row[col]) if str(row[col]).strip() not in ("", "nan") else 0.0
        for _, row in df.iterrows()
    }

min_thresholds = _thresh_map(summary_df, "Min Alert (m³)")
max_thresholds = _thresh_map(summary_df, "Max Daily (m³)")

alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    min_alert = min_thresholds.get(name, 0.0)
    max_daily = max_thresholds.get(name, 0.0)
    for _, row in meter_data[meter_data["Date"] >= SPIKE_DISPLAY_FROM].iterrows():
        usage = row["Daily Usage (m³)"]
        other_days = meter_data[meter_data["Date"] != row["Date"]]["Daily Usage (m³)"].tolist()
        avg = clean_avg(other_days)
        threshold = avg * 2.5
        over_avg = usage > threshold and usage > min_alert
        over_max = max_daily > 0 and usage > max_daily
        if over_avg or over_max:
            alerts.append({
                "Meter": name,
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Usage (m³)": round(usage, 4),
                "Clean Mean (m³)": round(avg, 4),
                "Threshold (m³)": round(threshold, 4),
                "Min Alert (m³)": round(min_alert, 4) if min_alert else "",
                "Daily Limit rec. (m³)": round(max_daily, 4) if max_daily else "",
            })

# --- Tabs ---
tab_usage, tab_billing = st.tabs(["📊 Usage", "💰 Billing"])

# ================================================================
# TAB 1: USAGE
# ================================================================
with tab_usage:

    # --- Spike Alert ---
    st.subheader("⚠️ Spike Alert / High Use")
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        most_recent_date = alerts_df["Date"].max()
        most_recent_prev = (pd.Timestamp(most_recent_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        recent_alerts = alerts_df[alerts_df["Date"] == most_recent_date]
        meter_list = ", ".join(sorted(recent_alerts["Meter"].unique()))
        st.error(f"⚠️ **Spike alert — period {most_recent_prev} ~16:30 → {most_recent_date} ~16:30:** {meter_list}")
        last_2_dates = sorted(alerts_df["Date"].unique())[-2:]
        st.dataframe(alerts_df[alerts_df["Date"].isin(last_2_dates)], use_container_width=True, hide_index=True)
    else:
        st.success("No unusual usage detected.")

    st.divider()

    # --- KPI row ---
    col1, col2, col3 = st.columns(3)
    total_usage = pd.to_numeric(summary_df[usage_col], errors="coerce").sum()
    col1.metric("Total Usage (all meters)", f"{total_usage:.1f} m³")
    col2.metric("Active Meters", len(summary_df))
    days = (daily_df["Date"].max() - daily_df["Date"].min()).days + 1
    col3.metric("Days Tracked", days)

    st.divider()

    # --- Daily Snapshot ---
    latest_date = daily_df["Date"].max()
    prev_date = latest_date - pd.Timedelta(days=1)
    st.subheader(f"📊 Daily Snapshot — {latest_date.strftime('%Y-%m-%d')}")
    st.caption(f"Period: {prev_date.strftime('%Y-%m-%d')} ~16:30 → {latest_date.strftime('%Y-%m-%d')} ~16:30")
    selected_snapshot = st.multiselect("Select meters", all_meters, default=all_meters, key="snapshot")
    if selected_snapshot:
        snapshot_df = daily_df[
            (daily_df["Date"] == latest_date) &
            (daily_df["Name"].isin(selected_snapshot))
        ].copy().sort_values("Daily Usage (m³)", ascending=False)
        alert_meters_today = {a["Meter"] for a in alerts if a["Date"] == latest_date.strftime("%Y-%m-%d")}

        def bar_color(row):
            usage = row["Daily Usage (m³)"]
            name = row["Name"]
            max_daily = max_thresholds.get(name, 0.0)
            if name in alert_meters_today:
                return "#E8443A"
            if max_daily > 0 and usage > max_daily:
                return "#E8443A"
            return "#4C9BE8"

        colors = snapshot_df.apply(bar_color, axis=1).tolist()
        fig_snapshot = px.bar(
            snapshot_df,
            x="Name",
            y="Daily Usage (m³)",
            labels={"Daily Usage (m³)": "Usage (m³)"},
        )
        fig_snapshot.update_traces(marker_color=colors)
        fig_snapshot.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_snapshot, use_container_width=True)

    st.divider()

    # --- Daily Usage Over Time ---
    st.subheader("📈 Daily Usage Over Time")
    st.caption("Each date is the end of the 24-hour reading period (previous day ~16:30 → that date ~16:30).")
    selected = st.multiselect("Select meters to display", all_meters, default=all_meters, key="timeseries")
    if selected:
        filtered = daily_df[daily_df["Name"].isin(selected)]
        fig_line = px.line(
            filtered,
            x="Date",
            y="Daily Usage (m³)",
            color="Name",
            markers=True,
            labels={"Daily Usage (m³)": "Daily Usage (m³)", "Date": "Date"},
        )
        fig_line.update_layout(hovermode="x unified")
        st.plotly_chart(fig_line, use_container_width=True)

    st.divider()

    # --- Total Daily Usage (since Feb 25) ---
    st.subheader("📈 Total System Usage — Since Feb 25, 2026")
    st.caption("Sum of all meters' daily usage per day (previous day ~16:30 → that date ~16:30).")
    total_daily = (
        daily_df[daily_df["Date"] >= pd.Timestamp("2026-02-25")]
        .groupby("Date", as_index=False)["Daily Usage (m³)"]
        .sum()
        .sort_values("Date")
    )
    total_mean = total_daily["Daily Usage (m³)"].mean()
    fig_total = px.line(
        total_daily,
        x="Date",
        y="Daily Usage (m³)",
        markers=True,
        labels={"Daily Usage (m³)": "Total Usage (m³)", "Date": "Date"},
    )
    fig_total.update_traces(line_color="#4C9BE8")
    fig_total.add_hline(
        y=total_mean,
        line_dash="dash",
        line_color="red",
        line_width=2,
        annotation_text=f"Mean: {total_mean:.2f} m³",
        annotation_position="top left",
    )
    fig_total.update_layout(hovermode="x unified")
    st.plotly_chart(fig_total, use_container_width=True)

    st.divider()

    # --- Meter vs Daily Limit ---
    st.subheader("📉 Meter Usage vs Daily Limit (rec)")
    st.caption("Last 30 days")
    selected_meter = st.selectbox("Select meter", all_meters, key="meter_vs_max")
    cutoff_30 = daily_df["Date"].max() - pd.Timedelta(days=29)
    meter_df = daily_df[(daily_df["Name"] == selected_meter) & (daily_df["Date"] >= cutoff_30)].copy()
    max_daily = max_thresholds.get(selected_meter, 0.0)
    if max_daily > 0 and not meter_df.empty:
        days_over = (meter_df["Daily Usage (m³)"] > max_daily).sum()
        total_days = len(meter_df)
        pct = days_over / total_days * 100
        avg_30 = meter_df["Daily Usage (m³)"].mean()
        avg_icon = "😊" if avg_30 <= max_daily else "😟"
        st.markdown(f"**{days_over} of {total_days} days - {pct:.1f}% - over Daily Limit (rec) of {max_daily:.2f} m³ | last 30 days daily avg: {avg_30:.2f} m³ {avg_icon}**")
    fig_mvmax = go.Figure()
    fig_mvmax.add_trace(go.Scatter(
        x=meter_df["Date"],
        y=meter_df["Daily Usage (m³)"],
        mode="lines+markers",
        line=dict(color="#1a4a8a", width=2),
        marker=dict(color="#1a4a8a", size=5),
        name="Daily Usage",
    ))
    if max_daily > 0:
        fig_mvmax.add_hline(
            y=max_daily,
            line_dash="dash",
            line_color="red",
            line_width=2,
            annotation_text=f"Daily Limit (rec): {max_daily:.2f} m³",
            annotation_position="top left",
        )
    fig_mvmax.update_layout(
        xaxis_title="Date",
        yaxis_title="Daily Usage (m³)",
        hovermode="x unified",
        showlegend=False,
    )
    st.plotly_chart(fig_mvmax, use_container_width=True)

    st.divider()

    # --- Total Usage per Meter ---
    st.subheader("📊 Total Usage per Meter (since Jan 6, 2026)")
    display_summary = summary_df.copy()
    numeric_cols = [c for c in ["Initial Reading (m³)", "Latest Total Flow (m³)", usage_col] if c in display_summary.columns]
    for col in numeric_cols:
        display_summary[col] = pd.to_numeric(display_summary[col], errors="coerce")
    summary_sorted = display_summary.sort_values(usage_col, ascending=False)
    fig_bar = px.bar(
        summary_sorted,
        x="Name",
        y=usage_col,
        color=usage_col,
        color_continuous_scale="Blues",
        labels={usage_col: "Total Usage (m³)"},
    )
    fig_bar.update_layout(xaxis_tickangle=-45, showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()

    # --- Meter Summary ---
    st.subheader("📋 Meter Summary")
    fmt = {c: "{:.4f}" for c in numeric_cols}
    st.dataframe(
        display_summary.style.format(fmt),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # --- Spike Log ---
    st.subheader("📋 Spike Log")
    st.caption("Automatically populated when alerts fire. Fill in Reason and Resolved directly in Google Sheets.")
    if not spike_df.empty:
        st.dataframe(spike_df, use_container_width=True, hide_index=True)
        st.markdown(
            "✏️ To add notes, open the **Spike Log** tab in your "
            "[Google Sheet](https://docs.google.com/spreadsheets/d/"
            "1I14yVDrcpY6C2tABWDSxZ_MRjAyyFjjFKlrC2JBwh0g/edit) and fill in the Reason and Resolved columns."
        )
    else:
        st.info("No spikes logged yet.")

# ================================================================
# TAB 2: BILLING
# ================================================================
with tab_billing:

    # --- Quarter selector ---
    current_month = pd.Timestamp.now().month
    current_year = pd.Timestamp.now().year
    current_q = (current_month - 1) // 3 + 1
    quarter_options = [f"Q{q} {current_year}" for q in range(1, 5)]
    default_q_index = current_q - 1
    selected_quarter = st.selectbox("Select Quarter", quarter_options, index=default_q_index)

    q_num = int(selected_quarter[1])
    q_year = int(selected_quarter[3:])
    q_start = pd.Timestamp(q_year, (q_num - 1) * 3 + 1, 1)
    q_end = (q_start + pd.offsets.QuarterEnd(0)).normalize()

    st.subheader(f"💰 Billing — {selected_quarter}")

    if variable_costs_error:
        st.warning(f"Could not load variable costs from Google Sheets: {variable_costs_error}")

    # --- Filter variable costs to quarter ---
    if not variable_costs_df.empty:
        q_costs_df = variable_costs_df[
            (variable_costs_df["Date"] >= q_start) &
            (variable_costs_df["Date"] <= q_end)
        ]
        q_costs_total = q_costs_df["Cost"].sum()
    else:
        q_costs_total = 0.0

    st.caption(
        f"Variable costs for {selected_quarter}: **${q_costs_total:,.2f}** | "
        f"Usage period: {q_start.strftime('%Y-%m-%d')} – {q_end.strftime('%Y-%m-%d')}"
    )

    # --- Filter usage to quarter ---
    q_usage = (
        daily_df[
            (daily_df["Date"] >= q_start) &
            (daily_df["Date"] <= q_end)
        ]
        .groupby("Name")["Daily Usage (m³)"]
        .sum()
        .reset_index()
        .rename(columns={"Daily Usage (m³)": "Usage (m³)"})
    )

    billing_df = summary_df[["Name", "Meter Number"]].merge(q_usage, on="Name", how="left")
    billing_df["Usage (m³)"] = pd.to_numeric(billing_df["Usage (m³)"], errors="coerce").fillna(0)
    total_system_usage = billing_df["Usage (m³)"].sum()
    if total_system_usage > 0:
        billing_df["% of Total Usage"] = (billing_df["Usage (m³)"] / total_system_usage * 100).round(2)
        billing_df["Variable Cost ($)"] = (billing_df["Usage (m³)"] / total_system_usage * q_costs_total).round(2)
    else:
        billing_df["% of Total Usage"] = 0.0
        billing_df["Variable Cost ($)"] = 0.0

    # Beginning reading: initial reading for Q1 2026, else first reading in quarter
    _initial_date = pd.Timestamp("2026-01-06")
    if q_start <= _initial_date:
        _begin_map = (
            summary_df.set_index("Name")["Initial Reading (m³)"]
            .apply(pd.to_numeric, errors="coerce")
            .to_dict()
        )
    else:
        _begin_map = (
            daily_df[daily_df["Date"] >= q_start]
            .sort_values("Date")
            .groupby("Name")
            .first()["Total Flow (m³)"]
            .to_dict()
        )
    # Ending reading: last available reading within the quarter
    _end_map = (
        daily_df[
            (daily_df["Date"] >= q_start) &
            (daily_df["Date"] <= q_end)
        ]
        .sort_values("Date")
        .groupby("Name")
        .last()["Total Flow (m³)"]
        .to_dict()
    )
    billing_df["Beginning Reading (m³)"] = billing_df["Name"].map(_begin_map)
    billing_df["Ending Reading (m³)"] = billing_df["Name"].map(_end_map)
    billing_df = billing_df[["Name", "Meter Number", "Beginning Reading (m³)", "Ending Reading (m³)", "Usage (m³)", "% of Total Usage", "Variable Cost ($)"]]

    billing_df = billing_df.sort_values("Usage (m³)", ascending=False)
    st.dataframe(
        billing_df.style.format({
            "Beginning Reading (m³)": "{:.4f}",
            "Ending Reading (m³)": "{:.4f}",
            "Usage (m³)": "{:.4f}",
            "% of Total Usage": "{:.2f}%",
            "Variable Cost ($)": "${:.2f}",
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.markdown(
        "📋 [View full Variable Costs ledger](https://docs.google.com/spreadsheets/d/"
        "1YHGambbpzGhSPttzOLpm04XKL4BN0GZhLTdw6VHFHcc/edit)"
    )

    st.divider()
    if not st.session_state.get("admin_authenticated"):
        with st.expander("⚙️ Admin", expanded=False):
            admin_pw = st.text_input("Admin password", type="password", key="admin_pw_input")
            if st.button("Unlock"):
                valid_admin = st.secrets.get("ADMIN_PASSWORD", os.environ.get("ADMIN_PASSWORD", ""))
                if admin_pw == valid_admin and valid_admin:
                    st.session_state["admin_authenticated"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password")
    else:
        with st.expander("⚙️ Admin — Generate Q2 Billing Tabs", expanded=False):
            st.caption(
                "Creates Q2 billing tabs in the LPV Water Meter Readings spreadsheet. "
                "Each tab includes Q2 fixed costs (forward-billed) and Q1 variable costs (backward-billed). "
                "Safe to re-run — existing Q2 tabs will be overwritten."
            )

            if st.button("🔍 List billing tabs (diagnostic)"):
                try:
                    spreadsheet = _get_statements_spreadsheet()
                    titles = [ws.title for ws in spreadsheet.worksheets()]
                    st.write("**All tabs found:**")
                    for t in titles:
                        st.code(t)
                except Exception as e:
                    st.error(f"Error connecting to spreadsheet: {e}")

            if st.button("Generate Q2 Billing Tabs", type="primary"):
                with st.spinner("Generating Q2 tabs…"):
                    try:
                        results, q1_var_total = generate_q2_billing_tabs(
                            daily_df, summary_df, variable_costs_df
                        )
                        st.success(f"Done! Q1 variable costs total used: **${q1_var_total:,.2f}**")
                        for r in results:
                            st.write(r)
                        st.markdown(
                            "📋 [Open LPV Water Meter Readings](https://docs.google.com/spreadsheets/d/"
                            "1CPztsWoAWVOPjDpJZMKTSMLdPo4ie8V0-pF6gUGxS7o/edit)"
                        )
                    except Exception as e:
                        st.error(f"Error: {e}")

# ================================================================
# FOOTER
# ================================================================
st.divider()
_logo_path = os.path.join(os.path.dirname(__file__), "../quick-export.png")
with open(_logo_path, "rb") as _f:
    _logo_b64 = base64.b64encode(_f.read()).decode()
if st.button("🔄 Refresh data", use_container_width=True):
    load_data.clear()
    load_spike_log.clear()
    load_variable_costs.clear()
    st.rerun()
st.markdown(
    f"<div style='text-align:center; padding:16px 0;'>"
    f"<img src='data:image/png;base64,{_logo_b64}' style='width:80px; height:auto;'>"
    f"</div>",
    unsafe_allow_html=True,
)
