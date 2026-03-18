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
    Calculate average daily usage from non-zero days only.
    Zeros represent unoccupied days and are excluded from the baseline
    so the threshold reflects actual usage when the property is in use.
    """
    if not daily_usages:
        return 0.0
    non_zero = [v for v in daily_usages if v > 0]
    return sum(non_zero) / len(non_zero) if non_zero else 0.0


def check_alerts(readings: list[dict], sheets_writer=None, min_thresholds: dict = None, max_thresholds: dict = None):
    """
    Send WhatsApp alert when either:
    - Usage > 3x clean average AND > min alert threshold (if set), OR
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
        threshold = avg_daily * 3

        # Check yesterday's reading against the clean average
        yesterday_rows = [r for r in rows if r["date"] == yesterday]
        if yesterday_rows:
            usage = yesterday_rows[0]["daily_usage"]
            min_alert = (min_thresholds or {}).get(name, 0.0)
            max_daily = (max_thresholds or {}).get(name, 0.0)
            over_avg = usage > threshold and usage > min_alert
            over_max = max_daily > 0 and usage > max_daily
            if over_avg or over_max:
                spike_alerts.append({"min_alert": min_alert,
                    "meter": name,
                    "usage": usage,
                    "normal_avg": avg_daily,
                    "threshold": threshold,
                    "date": yesterday,
                })

    if spike_alerts:
        msg = (
            "⚠️ LPV Water - SPIKE ALERT\n"
            f"High usage detected on {yesterday} (3x normal average):\n"
            + "\n".join(
                f"  • {s['meter']}: {s['usage']:.2f} m³ (normal avg {s['normal_avg']:.2f} m³/day)"
                for s in spike_alerts
            )
        )
        send_whatsapp(msg)
        logger.info(f"Spike alerts sent for {len(spike_alerts)} meters.")

        # Log each spike to the Spike Log sheet
        if sheets_writer:
            for spike in spike_alerts:
                sheets_writer.log_spike(spike)
    else:
        logger.info("No spike alerts triggered.")
