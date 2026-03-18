"""
LPV Water Meters - Streamlit Dashboard
Run: streamlit run src/dashboard.py
"""
import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../config/.env"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

st.set_page_config(
    page_title="LPV Water Meters",
    page_icon="💧",
    layout="wide",
)

st.markdown("""
    <style>
    /* Multiselect selected tag background */
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

    # Summary
    summary_ws = spreadsheet.worksheet("Summary")
    summary_df = pd.DataFrame(summary_ws.get_all_records())

    # Daily readings
    daily_ws = spreadsheet.worksheet("Daily Readings")
    daily_df = pd.DataFrame(daily_ws.get_all_records())
    daily_df["Date"] = pd.to_datetime(daily_df["Date"], errors="coerce")
    daily_df = daily_df.dropna(subset=["Date"])
    daily_df["Daily Usage (m³)"] = pd.to_numeric(daily_df["Daily Usage (m³)"], errors="coerce")
    daily_df["Total Flow (m³)"] = pd.to_numeric(daily_df["Total Flow (m³)"], errors="coerce")

    return summary_df, daily_df

@st.cache_data(ttl=60)
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

# --- Header ---
col_title, col_logo = st.columns([8, 1])
col_title.title("💧 LPV Water Meter Dashboard")
col_logo.image(os.path.join(os.path.dirname(__file__), "../lomaslogo.png"), width=80)
st.caption(f"Data from Feb 25, 2026 · Updates nightly · Last meter date: {daily_df['Date'].max().strftime('%Y-%m-%d')}")

if st.button("🔄 Refresh data"):
    load_data.clear()
    load_spike_log.clear()
    st.rerun()

st.divider()

# Detect usage column name dynamically
usage_col = next((c for c in summary_df.columns if "Total Usage" in c), None)
if usage_col is None:
    st.error("Could not find 'Total Usage Since' column in Summary sheet.")
    st.stop()

# --- Compute spikes early so banner can use them ---
def clean_avg(values):
    non_zero = [v for v in values if v > 0]
    return sum(non_zero) / len(non_zero) if non_zero else 0

SPIKE_DISPLAY_FROM = pd.Timestamp("2026-03-15")
all_meters = sorted(daily_df["Name"].unique())

# Build threshold lookups from Summary sheet
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
        threshold = avg * 3
        over_avg = usage > threshold and usage > min_alert
        over_max = max_daily > 0 and usage > max_daily
        if over_avg or over_max:
            alerts.append({
                "Meter": name,
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Usage (m³)": round(usage, 4),
                "Clean Avg (m³)": round(avg, 4),
                "Threshold (m³)": round(threshold, 4),
                "Min Alert (m³)": round(min_alert, 4) if min_alert else "",
                "Max Daily (m³)": round(max_daily, 4) if max_daily else "",
            })

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
total_usage = pd.to_numeric(summary_df[usage_col], errors="coerce").sum()
col1.metric("Total Usage (all meters)", f"{total_usage:.1f} m³")
col2.metric("Active Meters", len(summary_df))
top_user = summary_df.loc[pd.to_numeric(summary_df[usage_col], errors="coerce").idxmax(), "Name"]
col3.metric("Highest Usage", top_user)
days = (daily_df["Date"].max() - daily_df["Date"].min()).days + 1
col4.metric("Days Tracked", days)

# --- Spike banner — only show spikes from the most recent spike date ---
if alerts:
    alerts_df = pd.DataFrame(alerts)
    most_recent_date = alerts_df["Date"].max()
    recent_alerts = alerts_df[alerts_df["Date"] == most_recent_date]
    meter_list = ", ".join(sorted(recent_alerts["Meter"].unique()))
    st.error(
        f"⚠️ **Spike alert — {most_recent_date}:** {meter_list}  \n"
        "See the **Spike Alert / High Use** and **Spike Log** sections below for details."
    )

st.divider()

# --- 1. Latest day snapshot bar chart ---
latest_date = daily_df["Date"].max()
st.subheader(f"📊 Yesterday's Usage — {latest_date.strftime('%Y-%m-%d')}")

selected_snapshot = st.multiselect(
    "Select meters", all_meters, default=all_meters, key="snapshot"
)

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

# --- 2. Time series: daily usage ---
st.subheader("📈 Daily Usage Over Time")

selected = st.multiselect("Select meters to display", all_meters, default=all_meters[:5], key="timeseries")

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

# --- 3. Bar chart: total usage per meter ---
st.subheader("📊 Total Usage per Meter (since Feb 25, 2026)")
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

# --- 4. Meter Summary table ---
st.subheader("📋 Meter Summary")
fmt = {c: "{:.4f}" for c in numeric_cols}
st.dataframe(
    display_summary.style.format(fmt),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# --- 5. Spike Alert / High Use ---
st.subheader("⚠️ Spike Alert / High Use")

if alerts:
    st.dataframe(pd.DataFrame(alerts), use_container_width=True, hide_index=True)
else:
    st.success("No unusual usage detected.")

st.divider()

# --- 6. Spike Log ---
spike_col, spike_btn_col = st.columns([9, 1])
spike_col.subheader("📋 Spike Log")
if spike_btn_col.button("🔄 Refresh", key="refresh_spike"):
    load_spike_log.clear()
    spike_df = load_spike_log()
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
