"""
WhatsApp notifications via CallMeBot.
"""
import requests
import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)

# Recipients - phone: api_key
RECIPIENTS = {
    "50769276717": "4221726",
}


def send_whatsapp(message: str):
    """Send a WhatsApp message to all recipients."""
    for phone, apikey in RECIPIENTS.items():
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
    Calculate average daily usage excluding spikes.
    First pass: compute median-based average, then exclude values > 3x that average.
    This ensures the baseline reflects normal usage only.
    """
    if not daily_usages:
        return 0.0
    sorted_vals = sorted(daily_usages)
    mid = len(sorted_vals) // 2
    median = sorted_vals[mid]
    normal = [v for v in daily_usages if v <= median * 3]
    return sum(normal) / len(normal) if normal else median


def check_alerts(readings: list[dict]):
    """
    Send WhatsApp alert when today's usage exceeds 3x the meter's clean daily average.
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
        threshold = avg_daily * 3

        # Check yesterday's reading against the clean average
        yesterday_rows = [r for r in rows if r["date"] == yesterday]
        if yesterday_rows:
            usage = yesterday_rows[0]["daily_usage"]
            if usage > threshold:
                spike_alerts.append(
                    f"  • {name}: {usage:.2f} m³ "
                    f"(normal avg {avg_daily:.2f} m³/day)"
                )

    if spike_alerts:
        msg = (
            "⚠️ LPV Water - SPIKE ALERT\n"
            f"High usage detected on {yesterday} (3x normal average):\n"
            + "\n".join(spike_alerts)
        )
        send_whatsapp(msg)
        logger.info(f"Spike alerts sent for {len(spike_alerts)} meters.")
    else:
        logger.info("No spike alerts triggered.")
