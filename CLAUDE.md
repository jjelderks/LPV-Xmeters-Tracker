# LPV-Xmeters-Tracker

Water meter monitoring system for LPV (Lomas de Pedregal), a private water utility in Panama. Scrapes daily readings from xmeters.com and writes to Google Sheets. Meters installed Jan 6 2026; data collection starts Feb 25 2026.

## Environment

```bash
# Activate venv (always use this Python)
source .venv/bin/activate

# Install deps
pip install -r requirements.txt
```

Config lives in `config/.env` (not committed). Copy from `config/.env.example`.
Google service account key: `config/credentials.json` (not committed).

**Never commit `config/.env` or `config/credentials.json`.**

## Running things

```bash
# One-shot scrape + spike check
python src/main.py --once

# Streamlit dashboard (localhost:8501)
streamlit run src/dashboard.py

# Generate quarterly billing invoices
python src/generate_billing.py --quarter 2 --year 2026

# Scheduled daemon (runs daily at 06:00 via schedule lib)
python src/main.py
```

Cron runs at 23:00 UTC daily:
```
0 23 * * * cd /home/jjelderks/LPV-Xmeters-Tracker && .venv/bin/python src/main.py --once >> logs/cron.log 2>&1
```

## Key files

| File | Purpose |
|------|---------|
| `src/main.py` | Entry point, scheduler, spike-check backfill logic |
| `src/scraper.py` | REST API scraper — logs into xmeters.com API, pulls daily readings (no browser) |
| `src/sheets.py` | gspread wrapper — writes to Google Sheets |
| `src/notify.py` | Spike detection + WhatsApp alerts via CallMeBot |
| `src/dashboard.py` | Streamlit dashboard (usage charts, spike log) |
| `src/pages/mobile.py` | Mobile-optimized Streamlit page |
| `src/generate_billing.py` | Fills per-lot invoice tabs in the billing workbook |
| `src/explore.py` | Ad-hoc analysis script |

## Google Sheets

- **Meter tracking sheet** — ID in `config/.env` as `GOOGLE_SHEET_ID`
  - Tabs: `Daily Readings`, `Summary`, `Spike Log`
  - Summary has user-set per-meter min/max thresholds read before each write
- **Billing workbook** — ID hardcoded in `dashboard.py` and `generate_billing.py`
  - Tab order: Lot1–Lot26, LotS1–S9, Casita
  - Per-lot invoice tabs filled by `generate_billing.py`

## Spike alerts

Two triggers (both log to Spike Log sheet; only latest date sends WhatsApp):
1. Usage > 2.5× clean mean baseline AND > per-meter min threshold
2. Usage > 1.5× per-meter max daily (user-set in Summary sheet)

WhatsApp via CallMeBot — recipients configured in `src/notify.py::RECIPIENTS`.

## Billing rules

- Quarter N variable costs use Q(N-1) data
- Q1 2026 starts Jan 6 (meter install), not Jan 1
- Watch for Unicode issues in row matching (non-breaking spaces, m³ symbol)

## Coding conventions

- Scripts are standalone — runnable from repo root with the venv
- No ORM; use gspread directly
- All scripts load env from `config/.env` via `python-dotenv`
- Logs go to `logs/` directory
