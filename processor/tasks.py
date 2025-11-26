import time
import os
import re
import requests
from time import sleep
from celery import shared_task
from django import db as django_db
from datetime import datetime, timedelta
from django.core.management import call_command
from django.db import transaction, connection
from playwright.sync_api import sync_playwright
from .models import DemandData


@shared_task
def capture_demand_data_task():

    TARGET_URL = "https://vidyutpravah.in/state-data/tamil-nadu"
    API_ENDPOINT = "http://172.16.7.118:8003/api/tamilnadu/demand/post.demand.php"
    XPATH_CURRENT = '//*[@id="TamilNadu_map"]/div[6]/span/span'
    XPATH_YESTERDAY = '//*[@id="TamilNadu_map"]/div[4]/span/span'
    XPATH_TIME_BLOCK = '/html/body/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr/td[2]'

    SCREENSHOT_DIR = "screenshots"
    KEEP_DAYS = 2

    os.makedirs(SCREENSHOT_DIR, exist_ok=True)

    run_start_time = datetime.now()

    current_text = None
    yesterday_text = None
    parsed_time_block = None
    parsed_date_obj = None
    api_status = "UnknownError"

    # -------------------------------
    # BLOCK 1: WEB SCRAPING WITH RETRY (3 attempts)
    # -------------------------------
    MAX_RETRIES = 3
    retry_count = 0

    while retry_count < MAX_RETRIES:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.set_viewport_size({"width": 1920, "height": 1080})

                page.goto(TARGET_URL, timeout=90000, wait_until="domcontentloaded")
                page.wait_for_selector(f'xpath={XPATH_CURRENT}', timeout=30000)

                current_text = page.locator(f'xpath={XPATH_CURRENT}').inner_text()
                yesterday_text = page.locator(f'xpath={XPATH_YESTERDAY}').inner_text()
                print(yesterday_text, "yesterday")

                full_text = page.locator(f'xpath={XPATH_TIME_BLOCK}').inner_text()
                full_text = " ".join(full_text.split())

                match = re.search(
                    r"TIME BLOCK (\d{2}:\d{2} - \d{2}:\d{2}) DATED (\d{2} [A-Z]{3} \d{4})",
                    full_text
                )

                if match:
                    parsed_time_block = match.group(1)
                    parsed_date_obj = datetime.strptime(match.group(2), "%d %b %Y").date()
                    print(parsed_time_block, "parsed_time_block")
                    api_status = "DataCaptured"
                else:
                    api_status = "ParsingFailed"

                # Screenshot with timestamp
                if api_status == "DataCaptured":
                    timestamp = run_start_time.strftime("%Y-%m-%d_%H-%M-%S")
                    screenshot_file = f"vidyutpravah_{timestamp}.png"
                    screenshot_path = os.path.join(SCREENSHOT_DIR, screenshot_file)

                    page.screenshot(path=screenshot_path)

                    # CLEANUP OLD FILES
                    keep = {
                        (run_start_time.date() - timedelta(days=offset)).strftime("%Y-%m-%d")
                        for offset in range(KEEP_DAYS)
                    }

                    for f in os.listdir(SCREENSHOT_DIR):
                        if f.startswith("vidyutpravah_") and f.endswith(".png"):
                            date_part = f[13:23]
                            if date_part not in keep:
                                os.remove(os.path.join(SCREENSHOT_DIR, f))

                browser.close()
                break  # success → exit retry loop

        except Exception as e:
            retry_count += 1
            print(f"Scraping attempt {retry_count} failed:", e)

            if retry_count >= MAX_RETRIES:
                print("All scraping retries failed.")
                api_status = "ScrapingFailed"
                break

            time.sleep(3)

    # -------------------------------
    # BLOCK 2: PUSH DATA TO API
    # -------------------------------
    try:
        params = {"status": api_status}

        if api_status == "DataCaptured":
            print(current_text,"current_text")
            print(yesterday_text,"yesterday_text")
            params.update({
                "date": parsed_date_obj.strftime("%Y-%m-%d"),
                "time": parsed_time_block.replace(" ", ""),
                "current": current_text.replace(",", "").replace(" MW", "").strip(),
                "yesterday": yesterday_text.replace(",", "").replace(" MW", "").strip(),
            })

        requests.get(API_ENDPOINT, params=params, timeout=10)

    except Exception as e:
        print("API error:", e)

    # -------------------------------
    # BLOCK 3: SAVE TO DATABASE (4 attempts)
    # -------------------------------
    if api_status == "DataCaptured":

        attempts = 0
        max_attempts = 4

        while attempts < max_attempts:
            attempts += 1
            try:
                django_db.close_old_connections()

                if hasattr(connection, "ensure_connection"):
                    connection.ensure_connection()

                with transaction.atomic():
                    DemandData.objects.create(
                        current_demand=current_text,
                        yesterday_demand=yesterday_text,
                        time_block=parsed_time_block,
                        date=parsed_date_obj,
                    )

                return "Saved"

            except Exception as e:
                print("Database Error:", e)

                try:
                    connection.close()
                except:
                    pass

                # Wait before next retry
                time.sleep(2 ** attempts)

                if attempts == max_attempts:
                    return "DB_Save_Failed"

    return api_status


@shared_task(bind=True)
def run_management_commands(self, commands):
    """
    Run a list of management commands one by one.

    :param commands: List of management command names (without '.py') to run sequentially.
    """
    for command in commands:
        try:
            print(f"Running command: {command}")
            call_command(command)
            sleep(1)  # Optional: Sleep for a second between commands
        except Exception as e:
            self.retry(countdown=10, exc=e)
            print(f"Error running {command}: {e}")
        else:
            print(f"Successfully ran command: {command}")