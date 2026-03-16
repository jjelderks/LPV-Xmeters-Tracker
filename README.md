# LPV-Xmeters-Tracker

Scrapes meter reading data from a website and writes it to Google Sheets for tracking and graphing.

## Setup

### 1. Install dependencies
```bash
cd LPV-Xmeters-Tracker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp config/.env.example config/.env
# Edit config/.env with your site URL, credentials, and Google Sheet ID
```

### 3. Google Sheets credentials
- Go to [Google Cloud Console](https://console.cloud.google.com)
- Create a project → enable **Google Sheets API**
- Create a **Service Account** → download the JSON key
- Save it as `config/credentials.json`
- Share your Google Sheet with the service account email

### 4. Run

**Once (manual):**
```bash
python3 src/main.py --once
```

**Scheduled (runs daily at 06:00):**
```bash
python3 src/main.py
```

## Project structure
```
LPV-Xmeters-Tracker/
├── config/
│   ├── .env              # Your secrets (not committed)
│   ├── .env.example      # Template
│   └── credentials.json  # Google service account key (not committed)
├── data/                 # Local cache of downloaded files (optional)
├── logs/                 # Log output
├── src/
│   ├── main.py           # Entry point + scheduler
│   ├── scraper.py        # Website scraping logic
│   └── sheets.py         # Google Sheets writer
└── requirements.txt
```

## Next steps
1. Inspect the target website and implement `get_meter_readings()` in `src/scraper.py`
2. Set up Google Sheets credentials
3. Test with `python3 src/main.py --once`
