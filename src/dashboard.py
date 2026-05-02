"""
LPV Water Meters - Streamlit Dashboard
Run: streamlit run src/dashboard.py
"""
import os
import sys
import base64
import statistics
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


def _get_users() -> dict:
    if "users" in st.secrets:
        return {k: dict(v) for k, v in st.secrets["users"].items()}
    # legacy fallback
    return {
        st.secrets.get("USERNAME", "lpv_medidores").lower(): {
            "password": st.secrets.get("PASSWORD", "agua"),
            "role": "admin",
            "name": "Admin",
        }
    }


def _log_login(username: str, role: str):
    try:
        spreadsheet = _get_gspread_client()
        try:
            ws = spreadsheet.worksheet("Login Log")
        except Exception:
            ws = spreadsheet.add_worksheet("Login Log", rows=1000, cols=3)
            ws.append_row(["Timestamp", "Username", "Role"])
        ws.append_row([pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"), username, role])
    except Exception:
        pass


def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("💧 LPV Water Meters")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        users = _get_users()
        user = users.get(username.lower())
        if user and password == user["password"]:
            st.session_state["authenticated"] = True
            st.session_state["username"] = username.lower()
            st.session_state["role"] = user.get("role", "client")
            st.session_state["display_name"] = user.get("name", username)
            _log_login(username.lower(), user.get("role", "client"))
            st.rerun()
        else:
            st.error("Invalid username or password")
    return False


def is_admin() -> bool:
    return st.session_state.get("role") == "admin"


if not check_password():
    st.stop()

# --- Sidebar: user info + logout ---
with st.sidebar:
    display = st.session_state.get("display_name", st.session_state.get("username", ""))
    role = st.session_state.get("role", "client")
    st.markdown(f"**{display}** ({role})")
    if st.button("Logout"):
        for key in ["authenticated", "username", "role", "display_name"]:
            st.session_state.pop(key, None)
        st.rerun()

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
    daily_df["Total Flow (m³)"] = pd.to_numeric(daily_df["Total Flow (m³)"], errors="coerce")
    return summary_df, daily_df

@st.cache_data(ttl=3600)
def load_variable_costs():
    empty = pd.DataFrame(columns=["Date", "Category", "Vendor", "Item / Description", "Cost ($)"])
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
                    records.append({
                        "Date": date,
                        "Category": row[1].strip(),
                        "Vendor": row[2].strip(),
                        "Item / Description": row[3].strip(),
                        "Cost ($)": cost,
                    })
                except (ValueError, Exception):
                    pass
        return pd.DataFrame(records) if records else empty, None
    except Exception as e:
        return empty, str(e)


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
max_thresholds = _thresh_map(summary_df, "Max Daily (m³)")
high_thresholds = _thresh_map(summary_df, "🟡 High Warning (m³)")
critical_thresholds = _thresh_map(summary_df, "🔴 Critical (m³)")

# Spike alerts — statistical: usage > 2.5× clean mean for that meter
alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    min_alert = min_thresholds.get(name, 0.0)
    for _, row in meter_data[meter_data["Date"] >= SPIKE_DISPLAY_FROM].iterrows():
        usage = row["Daily Usage (m³)"]
        other_days = meter_data[meter_data["Date"] != row["Date"]]["Daily Usage (m³)"].tolist()
        avg = clean_avg(other_days)
        threshold = avg * 2.5
        if usage > threshold and usage > min_alert:
            alerts.append({
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Meter": name,
                "Usage (m³)": round(usage, 4),
                "Normal Avg (m³)": round(avg, 4),
                "Trigger": "2.5× avg exceeded",
            })

# Max Daily alerts — usage > Max Daily threshold
max_daily_alerts = []
for name in all_meters:
    meter_data = daily_df[daily_df["Name"] == name].copy()
    max_daily = max_thresholds.get(name, 0.0)
    if max_daily <= 0:
        continue
    for _, row in meter_data[meter_data["Date"] >= SPIKE_DISPLAY_FROM].iterrows():
        usage = row["Daily Usage (m³)"]
        if usage > max_daily:
            max_daily_alerts.append({
                "Date": row["Date"].strftime("%Y-%m-%d"),
                "Meter": name,
                "Usage (m³)": round(usage, 4),
                "Max Daily (m³)": f"{max_daily:.1f}",
            })

# Leak alerts — hard limit: usage > Critical threshold (burst pipe / stuck valve)
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
                "Usage (m³)": round(usage, 4),
                "Critical Limit (m³)": f"{critical:.1f}",
            })

# --- Tabs ---
tab_usage, tab_billing = st.tabs(["📊 Usage", "💰 Variable Billing"])

# ================================================================
# TAB 1: USAGE
# ================================================================
with tab_usage:

    # --- Spike Alert ---
    st.subheader("⚠️ Spike Alert")
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        most_recent_date = alerts_df["Date"].max()
        most_recent_prev = (pd.Timestamp(most_recent_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        recent_alerts = alerts_df[alerts_df["Date"] == most_recent_date]
        meter_list = ", ".join(sorted(recent_alerts["Meter"].unique()))
        st.error(f"⚠️ **Spike alert — period {most_recent_prev} ~16:30 → {most_recent_date} ~16:30:** {meter_list}")
        last_2_dates = sorted(alerts_df["Date"].unique())[-2:]
        disp_alerts = alerts_df[alerts_df["Date"].isin(last_2_dates)].copy()
        disp_alerts["Usage (m³)"] = disp_alerts["Usage (m³)"].apply(lambda x: f"{x:.2f}")
        disp_alerts["Normal Avg (m³)"] = disp_alerts["Normal Avg (m³)"].apply(lambda x: f"{x:.2f}")
        st.dataframe(disp_alerts, use_container_width=True, hide_index=True)
    else:
        st.success("No unusual usage detected.")

    # --- Max Daily Alert ---
    st.subheader("🔶 Max Daily Alert")
    if max_daily_alerts:
        md_df = pd.DataFrame(max_daily_alerts)
        most_recent_md = md_df["Date"].max()
        recent_md = md_df[md_df["Date"] == most_recent_md]
        md_meter_list = ", ".join(sorted(recent_md["Meter"].unique()))
        st.warning(f"🔶 **Over Max Daily — {most_recent_md}:** {md_meter_list}")
        last_2_md_dates = sorted(md_df["Date"].unique())[-2:]
        disp_md = md_df[md_df["Date"].isin(last_2_md_dates)].copy()
        disp_md["Usage (m³)"] = disp_md["Usage (m³)"].apply(lambda x: f"{x:.2f}")
        st.dataframe(disp_md, use_container_width=True, hide_index=True)
    else:
        st.success("No meters over Max Daily threshold.")

    # --- Leak / Hard Limit Alert ---
    st.subheader("🚨 Leak / Critical Limit")
    if leak_alerts:
        leak_df = pd.DataFrame(leak_alerts)
        most_recent_leak = leak_df["Date"].max()
        recent_leaks = leak_df[leak_df["Date"] == most_recent_leak]
        leak_meter_list = ", ".join(sorted(recent_leaks["Meter"].unique()))
        st.error(f"🚨 **Usage exceeded Critical threshold — {most_recent_leak}:** {leak_meter_list}")
        last_2_leak_dates = sorted(leak_df["Date"].unique())[-2:]
        disp_leaks = leak_df[leak_df["Date"].isin(last_2_leak_dates)].copy()
        disp_leaks["Usage (m³)"] = disp_leaks["Usage (m³)"].apply(lambda x: f"{x:.2f}")
        st.dataframe(disp_leaks, use_container_width=True, hide_index=True)
    else:
        st.success("No meters above Critical threshold.")

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
    available_dates = sorted(daily_df["Date"].unique(), reverse=True)
    available_dates_str = [d.strftime("%Y-%m-%d") for d in available_dates]
    st.subheader("📊 Daily Snapshot")
    selected_date_str = st.selectbox(
        "Date", available_dates_str, index=0, key="snapshot_date"
    )
    selected_date = pd.Timestamp(selected_date_str)
    prev_date = selected_date - pd.Timedelta(days=1)
    st.caption(f"Period: {prev_date.strftime('%Y-%m-%d')} ~16:30 → {selected_date_str} ~16:30")
    selected_snapshot = st.multiselect("Select meters", all_meters, default=all_meters, key="snapshot")
    if selected_snapshot:
        snapshot_df = daily_df[
            (daily_df["Date"] == selected_date) &
            (daily_df["Name"].isin(selected_snapshot))
        ].copy().sort_values("Daily Usage (m³)", ascending=False)
        alert_meters_today = {a["Meter"] for a in alerts if a["Date"] == selected_date_str}

        def bar_color(row):
            usage = row["Daily Usage (m³)"]
            name = row["Name"]
            critical = critical_thresholds.get(name, 0.0)
            if name in alert_meters_today:
                return "#E8443A"
            if critical > 0 and usage > critical:
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
    period_options = {"Last 30 days": 30, "Last 60 days": 60, "Last 90 days": 90}
    col_period, col_meters = st.columns([1, 3])
    with col_period:
        selected_period = st.selectbox("Period", list(period_options.keys()), index=1, key="timeseries_period")
    with col_meters:
        selected = st.multiselect("Select meters to display", all_meters, default=all_meters, key="timeseries")
    if selected:
        cutoff = latest_date - pd.Timedelta(days=period_options[selected_period] - 1)
        filtered = daily_df[daily_df["Name"].isin(selected) & (daily_df["Date"] >= cutoff)]
        fig_line = px.line(
            filtered,
            x="Date",
            y="Daily Usage (m³)",
            color="Name",
            markers=True,
            labels={"Daily Usage (m³)": "Daily Usage (m³)", "Date": "Date"},
            render_mode="svg",
        )
        fig_line.update_layout(hovermode="x unified")
        if len(selected) == 1:
            crit = critical_thresholds.get(selected[0], 0.0)
            if crit > 0:
                fig_line.add_hline(
                    y=crit,
                    line_dash="dash",
                    line_color="red",
                    line_width=2,
                    annotation_text=f"Critical Limit: {crit:.1f} m³",
                    annotation_position="top left",
                )
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
    critical_total = sum(critical_thresholds.get(name, 0.0) for name in all_meters)
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
        line_color="gray",
        line_width=2,
        annotation_text=f"System Mean: {total_mean:.2f} m³",
        annotation_position="top left",
    )
    fig_total.update_layout(hovermode="x unified")
    st.plotly_chart(fig_total, use_container_width=True)

    st.divider()

    # --- Water Usage vs Limits ---
    st.subheader("📉 Water Usage vs Limits")
    st.caption("Last 30 days")
    selected_meter = st.selectbox("Select meter", all_meters, key="meter_vs_max")
    cutoff_30 = daily_df["Date"].max() - pd.Timedelta(days=29)
    meter_df = daily_df[(daily_df["Name"] == selected_meter) & (daily_df["Date"] >= cutoff_30)].copy()
    max_daily_limit = max_thresholds.get(selected_meter, 0.0)
    if max_daily_limit > 0 and not meter_df.empty:
        days_over = (meter_df["Daily Usage (m³)"] > max_daily_limit).sum()
        total_days = len(meter_df)
        pct = days_over / total_days * 100
        avg_30 = meter_df["Daily Usage (m³)"].mean()
        avg_icon = "😊" if avg_30 <= max_daily_limit else "😟"
        st.markdown(f"**{days_over} of {total_days} days - {pct:.1f}% - over Max Daily ({max_daily_limit:.1f} m³) | last 30 days daily avg: {avg_30:.2f} m³ {avg_icon}**")
    fig_mvmax = go.Figure()
    fig_mvmax.add_trace(go.Scatter(
        x=meter_df["Date"],
        y=meter_df["Daily Usage (m³)"],
        mode="lines+markers",
        line=dict(color="#1a4a8a", width=2),
        marker=dict(color="#1a4a8a", size=5),
        name="Daily Usage",
    ))
    if max_daily_limit > 0:
        fig_mvmax.add_hline(
            y=max_daily_limit,
            line_dash="dash",
            line_color="red",
            line_width=2,
            annotation_text=f"Max Daily Threshold: {max_daily_limit:.1f} m³",
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
    for col in ["Bedrooms", "Max Daily (m³)", "Min Alert (m³)"]:
        if col in display_summary.columns:
            display_summary[col] = pd.to_numeric(display_summary[col], errors="coerce")
    fmt = {c: "{:.4f}" for c in numeric_cols}
    st.dataframe(
        display_summary.style.format(fmt, na_rep=""),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Bedrooms": st.column_config.NumberColumn("Beds", format="%.1f"),
            "Max Daily (m³)": st.column_config.NumberColumn("Max Daily (m³)", format="%.1f"),
            "Min Alert (m³)": st.column_config.NumberColumn("Min Alert (m³)", format="%.1f"),
        },
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

    st.subheader(f"💰 Variable Billing — {selected_quarter}")

    if variable_costs_error:
        st.warning(f"Could not load variable costs from Google Sheets: {variable_costs_error}")

    # --- Filter variable costs to quarter ---
    if not variable_costs_df.empty:
        q_costs_df = variable_costs_df[
            (variable_costs_df["Date"] >= q_start) &
            (variable_costs_df["Date"] <= q_end)
        ].copy()
        q_costs_total = q_costs_df["Cost ($)"].sum()
    else:
        q_costs_df = pd.DataFrame(columns=["Date", "Category", "Vendor", "Item / Description", "Cost ($)"])
        q_costs_total = 0.0

    st.caption(
        f"Variable costs for {selected_quarter}: **${q_costs_total:,.2f}** | "
        f"Usage period: {q_start.strftime('%Y-%m-%d')} – {q_end.strftime('%Y-%m-%d')}"
    )

    # --- Filter usage to quarter ---
    _meter_install = pd.Timestamp("2026-01-06")
    _is_q1_2026 = (q_start <= _meter_install <= q_end)

    if _is_q1_2026:
        # Daily data only starts Feb 25; use end_flow − initial_reading for true Q1 usage
        _q1_initial = (
            summary_df.set_index("Name")["Initial Reading (m³)"]
            .apply(pd.to_numeric, errors="coerce")
            .to_dict()
        )
        _q1_end = (
            daily_df[(daily_df["Date"] >= q_start) & (daily_df["Date"] <= q_end)]
            .sort_values("Date").groupby("Name").last()["Total Flow (m³)"]
            .to_dict()
        )
        q_usage = pd.DataFrame([
            {"Name": name, "Usage (m³)": (_q1_end.get(name) or 0) - (init or 0)}
            for name, init in _q1_initial.items()
            if pd.notna(init) and name in _q1_end
        ])
    else:
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
    st.divider()
    st.subheader(f"🧾 Variable Cost Breakdown — {selected_quarter}")
    if not q_costs_df.empty:
        display_costs = q_costs_df.copy()
        display_costs["Date"] = display_costs["Date"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            display_costs.style.format({"Cost ($)": "${:.2f}"}),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Total: **${q_costs_total:,.2f}**")
    else:
        st.info("No variable cost entries found for this quarter.")
    st.markdown(
        "📋 [View full Variable Costs ledger](https://docs.google.com/spreadsheets/d/"
        "1YHGambbpzGhSPttzOLpm04XKL4BN0GZhLTdw6VHFHcc/edit)"
    )


# ================================================================
# FOOTER
# ================================================================
st.divider()
_logo_path = os.path.join(os.path.dirname(__file__), "../quick-export.png")
with open(_logo_path, "rb") as _f:
    _logo_b64 = base64.b64encode(_f.read()).decode()
if is_admin() and st.button("🔄 Refresh data", use_container_width=True):
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
