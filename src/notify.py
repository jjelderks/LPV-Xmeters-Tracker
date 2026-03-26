"""
WhatsApp notifications via CallMeBot.
"""
import requests
import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)

# Recipients: phone, api_key, meters (None = all meters)
RECIPIENTS = [
    {"phone": "50769276717", "apikey": "4221726", "meters": None},
    {"phone": "972528022021", "apikey": "123123", "meters": ["S2 - Liron Casa", "S3 - Liron rental"]},
]


def send_whatsapp(message: str, phone: str, apikey: str):
    """Send a WhatsApp message to a single recipient."""
    try:
        url = (
            f"https://api.callmebot.com/whatsapp.php"
            f"?phone={phone}&text={quote(message)}&apikey={apikey}"
        )
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            logger.info(f"WhatsApp sent to {phone}")
        else:
            logger.warning(f"WhatsApp failed for {phone}: {resp.status_code} {resp.text[:100]}")
    except Exception as e:
        logger.error(f"WhatsApp error for {phone}: {e}")


def clean_average(daily_usages: list[float]) -> float:
    """
    Calculate average daily usage from non-zero days, excluding spike days.
    Zeros (unoccupied days) are excluded first, then days above 2.5x the
    median are excluded to prevent past spikes from inflating the baseline.
    """
    if not daily_usages:
        return 0.0
    non_zero = [v for v in daily_usages if v > 0]
    if not non_zero:
        return 0.0
    import statistics
    median = statistics.median(non_zero)
    filtered = [v for v in non_zero if v <= median * 2.5]
    return sum(filtered) / len(filtered) if filtered else sum(non_zero) / len(non_zero)


def check_alerts(readings: list[dict], sheets_writer=None, min_thresholds: dict = None, max_thresholds: dict = None):
    """
    Send WhatsApp alert when either:
    - Usage > 2.5x clean mean AND > min alert threshold (if set), OR
    - Usage > max daily threshold (unconditional, if set)
    """
    from collections import defaultdict
    from datetime import date, timedelta

    # Use yesterday's date since nightly data may not include today yet
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    by_meter = defaultdict(list)
    for r in readings:
        by_meter[r["name"]].append(r)

    spike_alerts = []

    for name, rows in by_meter.items():
        # All historical daily usages except yesterday (to build clean baseline)
        historical = [r["daily_usage"] for r in rows
                      if r["daily_usage"] > 0 and r["date"] != yesterday]
        if not historical:
            continue

        avg_daily = clean_average(historical)
        threshold = avg_daily * 2.5

        # Check yesterday's reading against the clean mean
        yesterday_rows = [r for r in rows if r["date"] == yesterday]
        if yesterday_rows:
            usage = yesterday_rows[0]["daily_usage"]
            min_alert = (min_thresholds or {}).get(name, 0.0)
            max_daily = (max_thresholds or {}).get(name, 0.0)
            over_avg = usage > threshold and usage > min_alert
            over_max = max_daily > 0 and usage > max_daily
            if over_avg or over_max:
                if over_avg and over_max:
                    trigger = f"2.5x clean mean + daily rec. exceeded ({max_daily:.2f} m³)"
                elif over_max:
                    trigger = f"daily rec. exceeded ({max_daily:.2f} m³)"
                else:
                    trigger = f"2.5x clean mean ({avg_daily:.2f} m³)"
                spike_alerts.append({
                    "meter": name,
                    "usage": usage,
                    "normal_avg": avg_daily,
                    "threshold": threshold,
                    "date": yesterday,
                    "trigger": trigger,
                })

    if spike_alerts:
        for recipient in RECIPIENTS:
            filtered = [
                s for s in spike_alerts
                if recipient["meters"] is None or s["meter"] in recipient["meters"]
            ]
            if filtered:
                from datetime import datetime, timedelta
                period_start = (datetime.strptime(yesterday, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                msg = (
                    "⚠️ LPV Water - HIGH USAGE ALERT\n"
                    f"Period: {period_start} ~16:30 → {yesterday} ~16:30\n"
                    + "\n".join(
                        f"  • {s['meter']}: {s['usage']:.2f} m³ ({s['trigger']})"
                        for s in filtered
                    )
                )
                send_whatsapp(msg, recipient["phone"], recipient["apikey"])
        logger.info(f"Spike alerts sent for {len(spike_alerts)} meters.")

        # Log each spike to the Spike Log sheet
        if sheets_writer:
            for spike in spike_alerts:
                sheets_writer.log_spike(spike)
    else:
        logger.info("No spike alerts triggered.")
