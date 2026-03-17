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

@st.cache_data(ttl=3600)
def load_data():
    # Cloud: use Streamlit secrets. Local: use credentials file.
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
    spreadsheet = client.open_by_key(sheet_id)

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

    # Spike log
    try:
        spike_ws = spreadsheet.worksheet("Spike Log")
        spike_df = pd.DataFrame(spike_ws.get_all_records())
    except Exception:
        spike_df = pd.DataFrame()

    return summary_df, daily_df, spike_df

# --- Load ---
with st.spinner("Loading data..."):
    summary_df, daily_df, spike_df = load_data()

# --- Header ---
col_title, col_logo = st.columns([8, 1])
col_title.title("💧 LPV Water Meter Dashboard")
col_logo.image(os.path.join(os.path.dirname(__file__), "../lomaslogo.png"), width=80)
st.caption(f"Data from Feb 25 · Updates nightly · Last meter date: {daily_df['Date'].max().strftime('%Y-%m-%d')}")

if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

# Detect usage column name dynamically
usage_col = next((c for c in summary_df.columns if c.startswith("Total Usage Since")), None)
if usage_col is None:
    st.error("Could not find 'Total Usage Since' column in Summary sheet.")
    st.stop()

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
total_usage = pd.to_numeric(summary_df[usage_col], errors="coerce").sum()
col1.metric("Total Usage (all meters)", f"{total_usage:.1f} m³")
col2.metric("Active Meters", len(summary_df))
top_user = summary_df.loc[pd.to_numeric(summary_df[usage_col], errors="coerce").idxmax(), "Name"]
col3.metric("Highest Usage", top_user)
days = (daily_df["Date"].max() - daily_df["Date"].min()).days + 1
col4.metric("Days Tracked", days)

st.divider()

# --- Summary table ---
st.subheader("📋 Meter Summary")
display_summary = summary_df.copy()
for col in ["Initial Reading (Jan 6)", "Latest Total Flow (m³)", usage_col]:
    display_summary[col] = pd.to_numeric(display_summary[col], errors="coerce")

st.dataframe(
    display_summary.style.format({
        "Initial Reading (Jan 6)": "{:.4f}",
        "Latest Total Flow (m³)": "{:.4f}",
        usage_col: "{:.4f}",
    }),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# --- Bar chart: total usage per meter ---
st.subheader("📊 Total Usage per Meter (since Feb 25)")
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

# --- Time series: daily usage ---
st.subheader("📈 Daily Usage Over Time")

all_meters = sorted(daily_df["Name"].unique())
selected = st.multiselect("Select meters to display", all_meters, default=all_meters[:5])

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

# --- High usage alerts ---
st.subheader("⚠️ Spike Alerts (3x clean daily average)")

def clean_avg(values):
    vals = sorted([v for v in values if v > 0])
    if not vals:
        return 0
    mid = vals[len(vals) // 2]
    normal = [v for v in vals if v <= mid * 3]
    return sum(normal) / len(normal) if normal else mid

SPIKE_DISPLAY_FROM = pd.Timestamp("2026-03-15")

alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    avg = clean_avg(meter_data["Daily Usage (m³)"].tolist())
    threshold = avg * 3
    # Use full history for baseline but only display from March 15
    high_days = meter_data[
        (meter_data["Daily Usage (m³)"] > threshold) &
        (meter_data["Date"] >= SPIKE_DISPLAY_FROM)
    ]
    for _, row in high_days.iterrows():
        alerts.append({
            "Meter": name,
            "Date": row["Date"].strftime("%Y-%m-%d"),
            "Usage (m³)": round(row["Daily Usage (m³)"], 4),
            "Clean Avg (m³)": round(avg, 4),
            "Threshold (m³)": round(threshold, 4),
        })

if alerts:
    st.dataframe(pd.DataFrame(alerts), use_container_width=True, hide_index=True)
else:
    st.success("No unusual usage detected.")

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
