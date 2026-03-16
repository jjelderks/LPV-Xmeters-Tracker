"""
exploration helper - opens xmeters.com in a visible browser so you can
inspect the login flow and dashboard layout.

Run: python3 src/explore.py
"""
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../config/.env"))

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=500)
    page = browser.new_page()

    print("Opening xmeters.com ...")
    page.goto(os.environ.get("SITE_URL", "https://xmeters.com"))
    page.wait_for_load_state("networkidle")

    print("Page title:", page.title())
    print("URL after load:", page.url)
    print()
    print("=== All input fields on the page ===")
    inputs = page.query_selector_all("input")
    for inp in inputs:
        print(
            f"  tag=input  type={inp.get_attribute('type')}  "
            f"name={inp.get_attribute('name')}  "
            f"id={inp.get_attribute('id')}  "
            f"placeholder={inp.get_attribute('placeholder')}"
        )

    print()
    print("=== All buttons ===")
    buttons = page.query_selector_all("button, input[type='submit']")
    for btn in buttons:
        print(f"  {btn.get_attribute('type') or 'button'}: {btn.inner_text()[:60]}")

    print()
    print("Browser is open - inspect the page, then press Enter to close.")
    input()
    browser.close()
