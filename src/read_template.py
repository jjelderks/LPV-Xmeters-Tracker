#!/usr/bin/env python3
"""Dump Lot1-Q226 tab contents with cell addresses for template mapping."""
import os
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../config/.env"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES)
client = gspread.authorize(creds)

wb = client.open("LPV Q1-Q2 2026 Invoices")
for tab in ["LotS9-Q226", "Lot18-21-Q226"]:
    ws = wb.worksheet(tab)
    rows = ws.get_all_values()
    print(f"\n=== {ws.title} ===")
    for i, row in enumerate(rows):
        for j, cell in enumerate(row):
            if str(cell).strip():
                col_letter = gspread.utils.rowcol_to_a1(i + 1, j + 1)[:-len(str(i + 1))]
                print(f"  {col_letter}{i+1:>3}  {repr(cell)}")
