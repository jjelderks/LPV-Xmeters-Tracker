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


def check_alerts(readings: list[dict], check_dates: list[str] = None, sheets_writer=None, min_thresholds: dict = None, max_thresholds: dict = None):
    """
    Check spike alerts for one or more dates and log/alert accordingly.

    check_dates: sorted list of dates to evaluate. Only the last date triggers
    WhatsApp. Older dates are backfilled into the Spike Log silently.
    Defaults to the single most recent date in readings.
    """
    from collections import defaultdict
    from datetime import datetime, timedelta

    all_dates = sorted({r["date"] for r in readings})
    if not all_dates:
        return

    if check_dates is None:
        check_dates = [all_dates[-1]]

    alert_date = check_dates[-1]  # only this date sends WhatsApp

    by_meter = defaultdict(list)
    for r in readings:
        by_meter[r["name"]].append(r)

    total_alerted = 0

    for check_date in check_dates:
        spike_alerts = []

        for name, rows in by_meter.items():
            # Build baseline from all data strictly before this date
            historical = [r["daily_usage"] for r in rows
                          if r["daily_usage"] > 0 and r["date"] < check_date]
            if not historical:
                continue

            avg_daily = clean_average(historical)
            threshold = avg_daily * 2.5

            date_rows = [r for r in rows if r["date"] == check_date]
            if not date_rows:
                continue

            usage = date_rows[0]["daily_usage"]
            min_alert = (min_thresholds or {}).get(name, 0.0)
            max_daily = (max_thresholds or {}).get(name, 0.0)
            alert_max = max_daily * 1.5 if max_daily > 0 else 0.0
            over_avg = usage > threshold and usage > min_alert
            over_max = alert_max > 0 and usage > alert_max
            if over_avg or over_max:
                if over_avg and over_max:
                    trigger = f"exceeded 2.5x clean mean (threshold {threshold:.2f} m³) + 1.5x daily limit ({alert_max:.2f} m³)"
                elif over_max:
                    trigger = f"exceeded 1.5x daily limit ({alert_max:.2f} m³)"
                else:
                    trigger = f"exceeded 2.5x clean mean (threshold {threshold:.2f} m³)"
                spike_alerts.append({
                    "meter": name,
                    "usage": usage,
                    "normal_avg": avg_daily,
                    "threshold": threshold,
                    "date": check_date,
                    "trigger": trigger,
                })

        # Send WhatsApp only for the most recent date
        if spike_alerts and check_date == alert_date:
            for recipient in RECIPIENTS:
                filtered = [
                    s for s in spike_alerts
                    if recipient["meters"] is None or s["meter"] in recipient["meters"]
                ]
                if filtered:
                    period_start = (datetime.strptime(check_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                    msg = (
                        "⚠️ LPV Water - HIGH USAGE ALERT\n"
                        f"Period: {period_start} ~16:30 → {check_date} ~16:30\n"
                        + "\n".join(
                            f"  • {s['meter']}: {s['usage']:.2f} m³ ({s['trigger']})"
                            for s in filtered
                        )
                    )
                    send_whatsapp(msg, recipient["phone"], recipient["apikey"])
            total_alerted += len(spike_alerts)
            logger.info(f"Spike alerts sent for {len(spike_alerts)} meters.")
        elif spike_alerts:
            logger.info(f"Backfill {check_date}: {len(spike_alerts)} spike(s) logged (no WhatsApp).")

        # Log all spikes to the Spike Log sheet regardless of date
        if sheets_writer:
            for spike in spike_alerts:
                sheets_writer.log_spike(spike)

    if not total_alerted and check_dates[-1] == alert_date:
        logger.info("No spike alerts triggered.")
