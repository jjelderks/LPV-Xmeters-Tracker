"""
LPV Water Meters - Mobile View
Simplified layout for phones and property manager use.
"""
import os
import json
import base64
import statistics
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    .js-plotly-plot .plotly { touch-action: pan-y !important; }
    h3 { font-size: 1.1rem !important; }
    </style>
""", unsafe_allow_html=True)

MOBILE_CHART_CONFIG = {
    "scrollZoom": False,
    "displayModeBar": False,
    "staticPlot": False,
}


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


def _get_users() -> dict:
    if "users" in st.secrets:
        return {k: dict(v) for k, v in st.secrets["users"].items()}
    return {
        st.secrets.get("USERNAME", "lpv_medidores"): {
            "password": st.secrets.get("PASSWORD", "agua"),
            "role": "admin",
            "name": "Admin",
        }
    }


def check_password():
    if st.session_state.get("authenticated"):
        return True
    _lp = os.path.join(os.path.dirname(__file__), "../../quick-export.png")
    with open(_lp, "rb") as _f:
        _lb64 = base64.b64encode(_f.read()).decode()
    st.markdown(
        f"<div style='text-align:center; margin-bottom:12px;'>"
        f"<img src='data:image/png;base64,{_lb64}' style='width:80px; height:auto;'></div>"
        f"<h2 style='text-align:center; font-size:1.3rem; margin:0;'>💧 LPV Water Meters</h2>",
        unsafe_allow_html=True,
    )
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login", use_container_width=True):
        users = _get_users()
        user = users.get(username.lower())
        if user and password == user["password"]:
            st.session_state["authenticated"] = True
            st.session_state["username"] = username.lower()
            st.session_state["role"] = user.get("role", "client")
            st.session_state["display_name"] = user.get("name", username)
            st.rerun()
        else:
            st.error("Invalid username or password")
    return False


def is_admin() -> bool:
    return st.session_state.get("role") == "admin"


if not check_password():
    st.stop()


@st.cache_data(ttl=3600)
def load_data():
    spreadsheet = _get_gspread_client()
    summary_ws = spreadsheet.worksheet("Summary")
    summary_df = pd.DataFrame(summary_ws.get_all_records())
    summary_df = summary_df[summary_df["Name"].astype(str).str.strip().str.len() > 0].reset_index(drop=True)
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
all_meters = sorted(daily_df["Name"].unique())


def _thresh_map(df, col):
    if col not in df.columns:
        return {}
    result = {}
    for _, row in df.iterrows():
        name = str(row["Name"]).strip()
        if not name:
            continue
        try:
            val = str(row[col]).strip()
            result[name] = float(val) if val not in ("", "nan") else 0.0
        except (ValueError, TypeError):
            pass
    return result


min_thresholds = _thresh_map(summary_df, "Min Alert (m³)")
critical_thresholds = _thresh_map(summary_df, "🔴 Critical (m³)")


def clean_avg(values):
    non_zero = [v for v in values if v > 0]
    if not non_zero:
        return 0
    median = statistics.median(non_zero)
    filtered = [v for v in non_zero if v <= median * 2.5]
    return sum(filtered) / len(filtered) if filtered else sum(non_zero) / len(non_zero)


SPIKE_DISPLAY_FROM = pd.Timestamp("2026-03-15")

# Spike alerts — statistical
alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    min_alert = min_thresholds.get(name, 0.0)
    for _, row in meter_data[meter_data["Date"] >= SPIKE_DISPLAY_FROM].iterrows():
        usage = row["Daily Usage (m³)"]
        other_days = meter_data[meter_data["Date"] != row["Date"]]["Daily Usage (m³)"].tolist()
        avg = clean_avg(other_days)
        if usage > avg * 2.5 and usage > min_alert:
            alerts.append({
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Meter": name,
                "Usage (m³)": round(usage, 2),
                "Normal Avg (m³)": round(avg, 2),
            })

# Leak alerts — hard critical limit
leak_alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    critical = critical_thresholds.get(name, 0.0)
    if critical <= 0:
        continue
    for _, row in meter_data[meter_data["Date"] >= SPIKE_DISPLAY_FROM].iterrows():
        usage = row["Daily Usage (m³)"]
        if usage > critical:
            leak_alerts.append({
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Meter": name,
                "Usage (m³)": round(usage, 2),
                "Critical Limit (m³)": f"{critical:.1f}",
            })

# --- Header ---
logo_path = os.path.join(os.path.dirname(__file__), "../../quick-export.png")
with open(logo_path, "rb") as _f:
    _logo_b64 = base64.b64encode(_f.read()).decode()
st.markdown(
    f"<h2 style='text-align:center; font-size:1.4rem; margin:0;'>💧 LPV Water Meter Dashboard</h2>"
    f"<p style='text-align:center; color:gray; font-size:0.8rem;'>Latest data: {latest_date.strftime('%Y-%m-%d')} · Updates nightly</p>",
    unsafe_allow_html=True,
)

st.divider()

# --- Spike banner ---
if alerts:
    alerts_df = pd.DataFrame(alerts)
    most_recent_date = alerts_df["Date"].max()
    recent_alerts = alerts_df[alerts_df["Date"] == most_recent_date]
    meter_list = ", ".join(sorted(recent_alerts["Meter"].unique()))
    st.error(f"⚠️ **Spike alert — {most_recent_date}:** {meter_list}")

if leak_alerts:
    leak_df_all = pd.DataFrame(leak_alerts)
    most_recent_leak = leak_df_all["Date"].max()
    recent_leaks = leak_df_all[leak_df_all["Date"] == most_recent_leak]
    leak_meter_list = ", ".join(sorted(recent_leaks["Meter"].unique()))
    st.error(f"🚨 **Critical limit exceeded — {most_recent_leak}:** {leak_meter_list}")

st.divider()

# --- KPIs ---
usage_col = next((c for c in summary_df.columns if "Total Usage" in c), None)
if usage_col:
    total_usage = pd.to_numeric(summary_df[usage_col], errors="coerce").sum()
    days = (daily_df["Date"].max() - daily_df["Date"].min()).days + 1
    st.markdown(f"""
    <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:8px;">
        <div style="background:#1e1e2e; border-radius:8px; padding:12px;">
            <div style="font-size:0.75rem; color:#aaa;">Total Usage</div>
            <div style="font-size:1.2rem; font-weight:bold;">{total_usage:.1f} m³</div>
        </div>
        <div style="background:#1e1e2e; border-radius:8px; padding:12px;">
            <div style="font-size:0.75rem; color:#aaa;">Active Meters</div>
            <div style="font-size:1.2rem; font-weight:bold;">{len(summary_df)}</div>
        </div>
        <div style="background:#1e1e2e; border-radius:8px; padding:12px;">
            <div style="font-size:0.75rem; color:#aaa;">Last Reading</div>
            <div style="font-size:1.2rem; font-weight:bold;">{latest_date.strftime('%Y-%m-%d')}</div>
        </div>
        <div style="background:#1e1e2e; border-radius:8px; padding:12px;">
            <div style="font-size:0.75rem; color:#aaa;">Days Tracked</div>
            <div style="font-size:1.2rem; font-weight:bold;">{days}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# --- Daily Snapshot ---
st.subheader(f"📊 Daily Snapshot — {latest_date.strftime('%Y-%m-%d')}")
snapshot_df = daily_df[daily_df["Date"] == latest_date].copy().sort_values("Daily Usage (m³)", ascending=False)
alert_meters_today = {a["Meter"] for a in alerts if a["Date"] == latest_date.strftime("%Y-%m-%d")}

def bar_color(row):
    critical = critical_thresholds.get(row["Name"], 0.0)
    if row["Name"] in alert_meters_today:
        return "#E8443A"
    if critical > 0 and row["Daily Usage (m³)"] > critical:
        return "#E8443A"
    return "#4C9BE8"

colors = snapshot_df.apply(bar_color, axis=1).tolist()
fig_snap = px.bar(snapshot_df, x="Name", y="Daily Usage (m³)", labels={"Daily Usage (m³)": "Usage (m³)"})
fig_snap.update_traces(marker_color=colors)
fig_snap.update_layout(xaxis_tickangle=-60, height=320, margin=dict(l=5, r=5, t=10, b=5), dragmode=False)
st.plotly_chart(fig_snap, use_container_width=True, config=MOBILE_CHART_CONFIG)

st.divider()

# --- Daily Usage Over Time ---
st.subheader("📈 Daily Usage Over Time")
default_meters = all_meters[:5]
selected = st.multiselect("Select meters", all_meters, default=default_meters)
if selected:
    filtered = daily_df[daily_df["Name"].isin(selected)]
    fig_line = px.line(filtered, x="Date", y="Daily Usage (m³)", color="Name", markers=True)
    fig_line.update_layout(
        height=300, margin=dict(l=5, r=5, t=10, b=5),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.4),
        dragmode=False,
    )
    st.plotly_chart(fig_line, use_container_width=True, config=MOBILE_CHART_CONFIG)

st.divider()

# --- Water Usage vs Limits ---
st.subheader("📉 Water Usage vs Limits")
st.caption("Last 30 days")
selected_meter = st.selectbox("Select meter", all_meters, key="meter_vs_max")
cutoff_30 = daily_df["Date"].max() - pd.Timedelta(days=29)
meter_df = daily_df[(daily_df["Name"] == selected_meter) & (daily_df["Date"] >= cutoff_30)].copy()
critical_limit = critical_thresholds.get(selected_meter, 0.0)
if critical_limit > 0 and not meter_df.empty:
    days_over = (meter_df["Daily Usage (m³)"] > critical_limit).sum()
    total_days = len(meter_df)
    pct = days_over / total_days * 100
    avg_30 = meter_df["Daily Usage (m³)"].mean()
    avg_icon = "😊" if avg_30 <= critical_limit else "😟"
    st.markdown(f"**{days_over} of {total_days} days - {pct:.1f}% - over Critical Limit ({critical_limit:.1f} m³) | 30d avg: {avg_30:.2f} m³ {avg_icon}**")
fig_mvmax = go.Figure()
fig_mvmax.add_trace(go.Scatter(
    x=meter_df["Date"], y=meter_df["Daily Usage (m³)"],
    mode="lines+markers",
    line=dict(color="#1a4a8a", width=2),
    marker=dict(color="#1a4a8a", size=5),
    name="Daily Usage",
))
if critical_limit > 0:
    fig_mvmax.add_hline(
        y=critical_limit, line_dash="dash", line_color="red", line_width=2,
        annotation_text=f"Critical Limit: {critical_limit:.1f} m³",
        annotation_position="top left",
    )
fig_mvmax.update_layout(
    height=300, margin=dict(l=5, r=5, t=10, b=5),
    xaxis_title="Date", yaxis_title="Usage (m³)",
    hovermode="x unified", showlegend=False, dragmode=False,
)
st.plotly_chart(fig_mvmax, use_container_width=True, config=MOBILE_CHART_CONFIG)

st.divider()

# --- Spike Alert ---
st.subheader("⚠️ Spike Alert")
if alerts:
    alerts_df = pd.DataFrame(alerts)
    last_2_dates = sorted(alerts_df["Date"].unique())[-2:]
    st.dataframe(alerts_df[alerts_df["Date"].isin(last_2_dates)], use_container_width=True, hide_index=True)
else:
    st.success("No unusual usage detected.")

# --- Leak / Critical Limit ---
st.subheader("🚨 Leak / Critical Limit")
if leak_alerts:
    leak_df = pd.DataFrame(leak_alerts)
    last_2_leak_dates = sorted(leak_df["Date"].unique())[-2:]
    st.dataframe(leak_df[leak_df["Date"].isin(last_2_leak_dates)], use_container_width=True, hide_index=True)
else:
    st.success("No meters above Critical threshold.")

st.divider()

# --- Spike Log ---
st.subheader("📋 Spike Log")
if not spike_df.empty:
    st.dataframe(spike_df, use_container_width=True, hide_index=True)
else:
    st.info("No spikes logged yet.")

st.divider()
if is_admin() and st.button("🔄 Refresh all data", use_container_width=True):
    load_data.clear()
    load_spike_log.clear()
    st.rerun()
st.markdown(
    f"<div style='text-align:center; padding:12px 0;'>"
    f"<img src='data:image/png;base64,{_logo_b64}' style='width:63px; height:auto;'>"
    f"</div>",
    unsafe_allow_html=True,
)
