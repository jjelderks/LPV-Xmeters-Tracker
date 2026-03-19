"""
LPV Water Meters - Mobile View
Simplified layout for phones and property manager use.
"""
import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import gspread

load_dotenv(os.path.join(os.path.dirname(__file__), "../../config/.env"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

st.set_page_config(
    page_title="LPV Water — Mobile",
    page_icon="💧",
    layout="centered",
)

st.markdown("""
    <style>
    [data-baseweb="tag"] { background-color: #4C7FAF !important; }
    .block-container { padding-top: 3rem; padding-bottom: 1rem; }

    /* Prevent plotly charts from capturing scroll on mobile */
    .js-plotly-plot .plotly { touch-action: pan-y !important; }

    /* Shrink "Daily Usage Over Time" subheader to fit one line */
    h3 { font-size: 1.1rem !important; }
    </style>
""", unsafe_allow_html=True)

MOBILE_CHART_CONFIG = {
    "scrollZoom": False,
    "displayModeBar": False,
    "staticPlot": False,
}


def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown(
        "<div style='text-align:center'><img src='app/static/lomaslogo.png' width='100'></div>",
        unsafe_allow_html=True,
    )
    st.title("💧 LPV Water Meters")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login", use_container_width=True):
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
with st.spinner("Loading..."):
    summary_df, daily_df = load_data()
    spike_df = load_spike_log()

latest_date = daily_df["Date"].max()

# --- Threshold lookups ---
def _thresh_map(df, col):
    if col not in df.columns:
        return {}
    return {
        row["Name"]: float(row[col]) if str(row[col]).strip() not in ("", "nan") else 0.0
        for _, row in df.iterrows()
    }

min_thresholds = _thresh_map(summary_df, "Min Alert (m³)")
max_thresholds = _thresh_map(summary_df, "Max Daily (m³)")
all_meters = sorted(daily_df["Name"].unique())

# --- Spike alert computation ---
def clean_avg(values):
    non_zero = [v for v in values if v > 0]
    return sum(non_zero) / len(non_zero) if non_zero else 0

SPIKE_DISPLAY_FROM = pd.Timestamp("2026-03-15")
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
                "Avg (m³)": round(avg, 4),
                "Threshold (m³)": round(threshold, 4),
            })

# --- Header: title + logo ---
import base64
logo_path = os.path.join(os.path.dirname(__file__), "../../lomaslogo.png")
with open(logo_path, "rb") as _f:
    _logo_b64 = base64.b64encode(_f.read()).decode()
st.markdown(
    f"<div style='display:flex; align-items:center; justify-content:space-between;'>"
    f"<h2 style='font-size:1.4rem; margin:0;'>💧 LPV Water Meter Dashboard</h2>"
    f"<img src='data:image/png;base64,{_logo_b64}' style='width:50px; height:auto;'>"
    f"</div>",
    unsafe_allow_html=True,
)
st.caption(f"<div style='text-align:center'>Latest data: {latest_date.strftime('%Y-%m-%d')} · Updates nightly</div>",
           unsafe_allow_html=True)

st.divider()

# --- 1. Spike banner ---
if alerts:
    alerts_df = pd.DataFrame(alerts)
    most_recent_date = alerts_df["Date"].max()
    recent_alerts = alerts_df[alerts_df["Date"] == most_recent_date]
    meter_list = ", ".join(sorted(recent_alerts["Meter"].unique()))
    st.error(
        f"⚠️ **Spike alert — {most_recent_date}:**\n\n{meter_list}\n\n"
        "See Spike Alert / High Use below."
    )

st.divider()

# --- 2. General info ---
usage_col = next((c for c in summary_df.columns if "Total Usage" in c), None)
if usage_col:
    total_usage = pd.to_numeric(summary_df[usage_col], errors="coerce").sum()
    c1, c2 = st.columns(2)
    c1.metric("Total Usage", f"{total_usage:.1f} m³")
    c2.metric("Active Meters", len(summary_df))
    c3, c4 = st.columns(2)
    c3.metric("Last Reading", latest_date.strftime("%Y-%m-%d"))
    days = (daily_df["Date"].max() - daily_df["Date"].min()).days + 1
    c4.metric("Days Tracked", days)

st.divider()

# --- 3. Yesterday's usage bar chart ---
st.subheader(f"📊 Daily Snapshot — {latest_date.strftime('%Y-%m-%d')}")

snapshot_df = daily_df[daily_df["Date"] == latest_date].copy().sort_values("Daily Usage (m³)", ascending=False)
alert_meters_today = {a["Meter"] for a in alerts if a["Date"] == latest_date.strftime("%Y-%m-%d")}

def bar_color(row):
    max_daily = max_thresholds.get(row["Name"], 0.0)
    if row["Name"] in alert_meters_today:
        return "#E8443A"
    if max_daily > 0 and row["Daily Usage (m³)"] > max_daily:
        return "#E8443A"
    return "#4C9BE8"

colors = snapshot_df.apply(bar_color, axis=1).tolist()
fig_snap = px.bar(
    snapshot_df,
    x="Name",
    y="Daily Usage (m³)",
    labels={"Daily Usage (m³)": "Usage (m³)"},
)
fig_snap.update_traces(marker_color=colors)
fig_snap.update_layout(
    xaxis_tickangle=-60,
    height=320,
    margin=dict(l=5, r=5, t=10, b=5),
    dragmode=False,
)
st.plotly_chart(fig_snap, use_container_width=True, config=MOBILE_CHART_CONFIG)

st.divider()

# --- 4. Daily Usage Over Time ---
st.subheader("📈 Daily Usage Over Time")
default_meters = all_meters[:5]
selected = st.multiselect("Select meters", all_meters, default=default_meters)
if selected:
    filtered = daily_df[daily_df["Name"].isin(selected)]
    fig_line = px.line(
        filtered,
        x="Date",
        y="Daily Usage (m³)",
        color="Name",
        markers=True,
    )
    fig_line.update_layout(
        height=300,
        margin=dict(l=5, r=5, t=10, b=5),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.4),
        dragmode=False,
    )
    st.plotly_chart(fig_line, use_container_width=True, config=MOBILE_CHART_CONFIG)

st.divider()

# --- 5. Spike Alert / High Use ---
st.subheader("⚠️ Spike Alert / High Use")
if alerts:
    st.dataframe(pd.DataFrame(alerts), use_container_width=True, hide_index=True)
else:
    st.success("No unusual usage detected.")

st.divider()

# --- 6. Spike Log ---
spike_col, spike_btn_col = st.columns([4, 1])
spike_col.subheader("📋 Spike Log")
if spike_btn_col.button("🔄", key="refresh_spike"):
    load_spike_log.clear()
    spike_df = load_spike_log()

if not spike_df.empty:
    st.dataframe(spike_df, use_container_width=True, hide_index=True)
else:
    st.info("No spikes logged yet.")

st.divider()
if st.button("🔄 Refresh all data", use_container_width=True):
    load_data.clear()
    load_spike_log.clear()
    st.rerun()
