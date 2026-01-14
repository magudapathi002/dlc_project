from django.core.management.base import BaseCommand
import os
import requests
from tabula.io import read_pdf
import json
import re
import tempfile
from datetime import datetime, timedelta
from ...models import PosocoTableA, PosocoTableG
from PyPDF2 import PdfReader
import shutil

import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.poolmanager import PoolManager



class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = create_urllib3_context()

        # 🔥 CRITICAL FIX
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        # Allow legacy renegotiation
        ctx.options |= 0x4

        # Allow weak ciphers (gov sites)
        try:
            ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        except Exception:
            pass

        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )


# --- Constants ---
API_URL = "https://webapi.grid-india.in/api/v1/file"
BASE_URL = "https://webcdn.grid-india.in/"
SAVE_DIR = "downloads/POSOCO"

# --- payload: initial (today) - but we will fall back if API returns nothing ---
payload = {
    "_source": "GRDW",
    "_type": "DAILY_PSP_REPORT",
    "_fileDate": datetime.now().strftime("%Y-%m-%d"),
    "_month": datetime.now().strftime("%m")
}

# --- Helper Functions ---
def make_report_dir(base_dir, desired_date=None):
    """Create a timestamped subfolder inside POSOCO/. 
       If desired_date is provided, include that date in the folder name so past-date runs are kept distinct.
    """
    # desired_date may be a datetime.date or datetime or string parsed outside
    if desired_date:
        try:
            if isinstance(desired_date, datetime):
                date_part = desired_date.strftime("%Y-%m-%d")
            else:
                # assume date-like (datetime.date)
                date_part = desired_date.strftime("%Y-%m-%d")
        except Exception:
            date_part = datetime.now().strftime("%Y-%m-%d")
    else:
        date_part = datetime.now().strftime("%Y-%m-%d")

    timestamp = datetime.now().strftime("%H-%M-%S")
    # folder includes the requested date and a time suffix to avoid collisions
    report_dir = os.path.join(base_dir, f"report_{date_part}_{timestamp}")
    os.makedirs(report_dir, exist_ok=True)
    return report_dir, timestamp


def _parse_date_from_string(s):
    """Try several date patterns and return a datetime or None."""
    if not s:
        return None
    s = str(s)
    patterns = [
        (r'(\d{4}-\d{2}-\d{2})', '%Y-%m-%d'),
        (r'(\d{2}-\d{2}-\d{4})', '%d-%m-%Y'),
        (r'(\d{2}\d{2}\d{4})', '%d%m%Y'),
        (r'(\d{8})', '%Y%m%d'),
    ]
    for pat, fmt in patterns:
        m = re.search(pat, s)
        if m:
            try:
                return datetime.strptime(m.group(1), fmt)
            except Exception:
                continue
    try:
        return datetime.fromisoformat(s.split(".")[0])
    except Exception:
        return None


def _post_and_get_retdata(api_url, payload, timeout=30):
    """POST and return parsed JSON (or None on failure)."""
    try:
        session = requests.Session()
        session.mount('https://', LegacySSLAdapter())
        resp = session.post(
            api_url,
            json=payload,
            timeout=timeout,
            verify=False
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Network/API error when posting payload {payload}: {e}")
        return None
    except ValueError as e:
        print(f"❌ Invalid JSON response for payload {payload}: {e}")
        return None


def _download_to_temp(url, timeout=60):
    """Download URL to a temporary file and return its path (or None)."""
    try:
        session = requests.Session()
        session.mount('https://', LegacySSLAdapter())
        r = session.get(
            url,
            stream=True,
            timeout=timeout,
            verify=False
        )
        r.raise_for_status()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        with open(tmp.name, "wb") as fh:
            for chunk in r.iter_content(1024):
                fh.write(chunk)
        return tmp.name
    except Exception as e:
        print(f"❌ Error downloading temp PDF {url}: {e}")
        return None


def _extract_report_date_from_pdf(pdf_path):
    """
    Read first page text of pdf_path and try to find a date of the form:
    DD.MM.YYYY or DD-MM-YYYY or YYYY-MM-DD or DD/MM/YYYY
    Returns a datetime.date or None.
    """
    if PdfReader is None:
        # PyPDF2 not available
        return None
    try:
        reader = PdfReader(pdf_path)
        if len(reader.pages) == 0:
            return None
        first_page = reader.pages[0]
        text = ""
        try:
            text = first_page.extract_text() or ""
        except Exception:
            # fallback if extract_text fails
            text = ""
        if not text:
            return None
        # search common date patterns
        patterns = [
            r'(\b\d{2}[.\-/]\d{2}[.\-/]\d{4}\b)',  # DD.MM.YYYY or DD-MM-YYYY or DD/MM/YYYY
            r'(\b\d{4}[.\-/]\d{2}[.\-/]\d{2}\b)',  # YYYY-MM-DD
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                ds = m.group(1)
                for fmt in ("%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        dt = datetime.strptime(ds, fmt)
                        return dt.date()
                    except Exception:
                        continue
        return None
    except Exception as e:
        print(f"❌ Error reading PDF for printed date: {e}")
        return None


# ---------- New: fetch_report_for_target_date_with_fill ----------
def fetch_report_for_target_date_with_fill(api_url, base_url, payload, report_dir, target_date, lookback_days=7):
    """
    Policy:
      - If a PDF for report_date == target_date exists and is valid, download and save as posoco_<target_date>.pdf
      - Else choose the PDF with posting_date <= target_date with the latest posting_date (i.e. most recent available by that day). 
        Download that PDF (even if its report_date < target_date) and SAVE the file named as posoco_<target_date>.pdf.
      - Else look back by report_date up to lookback_days and pick the latest previous report; save it named as posoco_<target_date>.pdf.
    Returns: (local_pdf_path_or_None, metadata_or_None)
    metadata = { 'selected_report_date': date, 'selected_posting_date': date, 'title': str, 'filepath': str, 'mime': str }
    """
    try:
        resp = _post_and_get_retdata(api_url, {"_source": payload.get("_source", "GRDW"), "_type": payload.get("_type", "DAILY_PSP_REPORT")})
        if not resp or not resp.get("retData"):
            print("❌ No retData from API.")
            return None, None

        items = resp["retData"]

        def parse_title_to_date(title):
            if not title:
                return None
            m = re.match(r'^\s*(\d{2})[.\-/](\d{2})[.\-/](\d{2,4})', str(title).strip())
            if not m:
                return None
            d, mo, yy = m.group(1), m.group(2), m.group(3)
            yyyy = 2000 + int(yy) if len(yy) == 2 else int(yy)
            try:
                return datetime(yyyy, int(mo), int(d)).date()
            except Exception:
                return None

        def parse_posting_date(it):
            for fld in ("CreatedOn", "ModifiedOn", "Field1", "Field2"):
                val = it.get(fld)
                if val:
                    p = _parse_date_from_string(val)
                    if p:
                        return p.date() if isinstance(p, datetime) else p.date()
            return None

        # Build enriched records
        records = []
        for it in items:
            mime = (it.get("MimeType") or "").lower()
            title = (it.get("Title_") or it.get("FileName") or "") or ""
            report_dt = parse_title_to_date(title)
            posting_dt = parse_posting_date(it)
            file_path = it.get("FilePath") or it.get("Path") or it.get("Filepath")
            records.append({
                "item": it,
                "title": title,
                "mime": mime,
                "report_date": report_dt,
                "posting_date": posting_dt,
                "filepath": file_path
            })

        # Focus on real PDFs (prefer actual pdf records)
        pdf_records = [r for r in records if r["filepath"] and r["mime"] == "application/pdf"]
        # If no pdf_records, try to find pdf alternatives (same title)
        if not pdf_records:
            pdf_alts = []
            for r in records:
                if r["title"]:
                    alt = next((x for x in records if x["title"] == r["title"] and x["mime"] == "application/pdf" and x["filepath"]), None)
                    if alt:
                        pdf_alts.append(alt)
            pdf_records = pdf_alts

        if not pdf_records:
            print("⚠️ No PDF records available in retData.")
            return None, None

        # 1) Try exact report_date == target_date among PDFs
        exacts = [r for r in pdf_records if r["report_date"] == target_date]
        if exacts:
            for r in sorted(exacts, key=lambda x: (x["posting_date"] or datetime(1970,1,1).date()), reverse=True):
                fp = r["filepath"]
                download_url = base_url.rstrip("/") + "/" + fp.lstrip("/")
                if "?" not in download_url:
                    download_url = download_url + f"?cachebust={int(datetime.now().timestamp())}"
                tmp = _download_to_temp(download_url)
                if not tmp:
                    continue
                printed = _extract_report_date_from_pdf(tmp)
                if printed:
                    if printed == r["report_date"]:
                        dest = os.path.join(report_dir, f"posoco_{target_date.strftime('%d%m%Y')}.pdf")
                        shutil.move(tmp, dest)
                        meta = {"selected_report_date": r["report_date"], "selected_posting_date": r["posting_date"], "title": r["title"], "filepath": fp, "mime": r["mime"]}
                        print(f"✅ Exact match downloaded and saved as {dest}")
                        return dest, meta
                    else:
                        print(f"   ⚠️ Exact candidate printed date {printed} != expected {r['report_date']} — rejecting candidate.")
                        try:
                            os.remove(tmp)
                        except Exception:
                            pass
                        continue
                else:
                    # accept exact-match even if printed date couldn't be extracted
                    dest = os.path.join(report_dir, f"posoco_{target_date.strftime('%d%m%Y')}.pdf")
                    shutil.move(tmp, dest)
                    meta = {"selected_report_date": r["report_date"], "selected_posting_date": r["posting_date"], "title": r["title"], "filepath": fp, "mime": r["mime"]}
                    print(f"⚠️ Exact match PDF lacked printed date but Title matches — saved as {dest}")
                    return dest, meta

        # 2) No exact valid PDF for target_date — select PDFs posted ON OR BEFORE target_date
        posted_before = [r for r in pdf_records if r["posting_date"] and r["posting_date"] <= target_date]
        if posted_before:
            chosen = sorted(posted_before, key=lambda x: (x["posting_date"], x["report_date"] or datetime(1970,1,1).date()), reverse=True)[0]
            fp = chosen["filepath"]
            download_url = base_url.rstrip("/") + "/" + fp.lstrip("/")
            if "?" not in download_url:
                download_url = download_url + f"?cachebust={int(datetime.now().timestamp())}"
            tmp = _download_to_temp(download_url)
            if not tmp:
                print("❌ Failed to download chosen posted candidate.")
                return None, None
            # IMPORTANT: save file named by target_date as user asked
            dest = os.path.join(report_dir, f"posoco_{target_date.strftime('%d%m%Y')}.pdf")
            shutil.move(tmp, dest)
            meta = {"selected_report_date": chosen["report_date"], "selected_posting_date": chosen["posting_date"], "title": chosen["title"], "filepath": fp, "mime": chosen["mime"]}
            print(f"✅ Selected most-recent posted-by-{target_date} report (actual report_date={chosen['report_date']}, posting_date={chosen['posting_date']}) and saved as {dest}")
            return dest, meta

        # 3) Nothing posted by target_date — look back by report_date up to lookback_days
        for delta in range(1, lookback_days + 1):
            prev = target_date - timedelta(days=delta)
            candidates_prev = [r for r in pdf_records if r["report_date"] == prev]
            if candidates_prev:
                chosen = sorted(candidates_prev, key=lambda x: (x["posting_date"] or datetime(1970,1,1).date()), reverse=True)[0]
                fp = chosen["filepath"]
                download_url = base_url.rstrip("/") + "/" + fp.lstrip("/")
                if "?" not in download_url:
                    download_url = download_url + f"?cachebust={int(datetime.now().timestamp())}"
                tmp = _download_to_temp(download_url)
                if not tmp:
                    continue
                # Save using requested target_date filename (user requested)
                dest = os.path.join(report_dir, f"posoco_{target_date.strftime('%d%m%Y')}.pdf")
                shutil.move(tmp, dest)
                meta = {"selected_report_date": chosen["report_date"], "selected_posting_date": chosen["posting_date"], "title": chosen["title"], "filepath": fp, "mime": chosen["mime"]}
                print(f"ℹ️ No report posted by {target_date}; fetched previous report {chosen['report_date']} and saved AS {dest}")
                return dest, meta

        # 4) nothing found
        print(f"❌ No suitable PDF available for {target_date} (and no previous reports within {lookback_days} days).")
        return None, None

    except Exception as e:
        print(f"❌ Error in fetch_report_for_target_date_with_fill: {e}")
        return None, None


def extract_tables_from_pdf(pdf_file, report_dir, timestamp, desired_date=None):
    """
    Extracts tables, renames headings, and saves as JSON.
    This version uses flexible matching to handle unpredictable keys.
    """
    def get_short_key_simple(long_key):
        key = long_key.strip()
        
        # We check for the most specific keys first to avoid errors
        if key.startswith("Demand Met during Evening Peak"):
            return "demand_evening_peak"
        if key.startswith("Energy Shortage"):
            return "energy_shortage"
        if key.startswith("Maximum Demand Met During the Day"):
            return "max_demand_day"
        if key.startswith("Time Of Maximum Demand Met"):
            return "time_of_max_demand"
        if key.startswith("Peak Shortage"):
            return "peak_shortage"
        if key.startswith("Energy Met"):
            return "energy"
        if key.startswith("Hydro Gen"):
            return "hydro"
        if key.startswith("Wind Gen"):
            return "wind"
        if key.startswith("Solar Gen"):
            return "solar"
        if key == "Coal":
            return "coal"
        if key == "Lignite":
            return "lignite"
        if key == "Hydro":
            return "hydro"
        if key == "Nuclear":
            return "nuclear"
        if key == "Gas, Naptha & Diesel":
            return "gas_naptha_diesel"
        if key.startswith("RES"):
            return "res_total"
        if key == "Total":
            return "total"
            
        return key # Fallback to the original key if no match is found

    try:
        tables = read_pdf(pdf_file, pages="all", multiple_tables=True, lattice=True)
    except Exception as e:
        print(f"❌ Error reading PDF with Tabula: {e}")
        tables = []

    final_json = {"POSOCO": {"posoco_table_a": [], "posoco_table_g": []}}

    # Initialize variables to hold the found tables
    table_a_df = None
    table_g_df = None

    # Loop through all extracted tables to find the ones we need
    for df in tables:
        # Skip empty or invalid dataframes
        if df.empty or len(df.columns) == 0:
            continue

        # Convert the first column to string type for reliable searching
        first_col_str = df.iloc[:, 0].astype(str)

        # Identify Table A by looking for a unique phrase in its first column
        if any("Demand Met during Evening Peak" in s for s in first_col_str):
            print("✅ Found Table A by its content.")
            table_a_df = df

        # Identify Table G by looking for "Coal", a unique keyword for the fuel table
        elif any("Coal" in s for s in first_col_str):
            print("✅ Found Table G by its content.")
            table_g_df = df

        # If we have found both tables, we can stop searching
        if table_a_df is not None and table_g_df is not None:
            break

    # Process Table A if it was found
    if table_a_df is not None:
        table_a_df = table_a_df.set_index(table_a_df.columns[0]).dropna(how='all')
        table_a_dict = {}
        for original_key, row in table_a_df.iterrows():
            clean_key = ' '.join(str(original_key).replace('\r', ' ').split())
            short_key = get_short_key_simple(clean_key) # Use the new simple function
            table_a_dict[short_key] = row.dropna().to_dict()
        final_json["POSOCO"]["posoco_table_a"].append(table_a_dict)

    # Process Table G if it was found
    if table_g_df is not None:
        table_g_df = table_g_df.set_index(table_g_df.columns[0]).dropna(how='all')
        table_g_dict = {}
        for original_key, row in table_g_df.iterrows():
            clean_key = str(original_key).strip()
            short_key = get_short_key_simple(clean_key) # Use the new simple function
            table_g_dict[short_key] = row.dropna().to_dict()
        final_json["POSOCO"]["posoco_table_g"].append(table_g_dict)

    # Check if BOTH tables were not found, and if so, use the empty template.
    if table_a_df is None and table_g_df is None:
        final_json = {
            "POSOCO": {
                "posoco_table_a": [{"demand_evening_peak": None, "peak_shortage": None, "energy": None, "hydro": None, "wind": None, "solar": None, "energy_shortage": None, "max_demand_day": None, "time_of_max_demand": None}],
                "posoco_table_g": [{"coal": None, "lignite": None, "hydro": None, "nuclear": None, "gas_naptha_diesel": None, "res_total": None, "total": None}]
            }
        }
        print("⚠️ No valid tables found in PDF. Using empty template.")

    # Prefer the requested desired_date for JSON filename; fallback to now()
    if desired_date:
        if isinstance(desired_date, datetime):
            date_compact = desired_date.strftime('%d%m%Y')
        else:
            # assume date-like (datetime.date)
            date_compact = desired_date.strftime('%d%m%Y')
    else:
        date_compact = datetime.now().strftime('%d%m%Y')

    json_name = f"posoco_{date_compact}.json"
    output_json = os.path.join(report_dir, json_name)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(final_json, f, indent=4, ensure_ascii=False)

    print(f"✅ JSON with shortened keys saved successfully at: {output_json}")

    return final_json


def save_to_db(final_json, report_date=None):
    """Saves the processed JSON data to the Django database."""
    # If report_date is provided, use that; otherwise use today
    today = report_date or datetime.now().date()

    try:
        # Save data from Table A
        table_a_data = final_json.get("posoco_table_a", [])
        if table_a_data and table_a_data[0]:
            for category, values in table_a_data[0].items():
                if values is None or not isinstance(values, dict):
                    continue
                if all(v is None for v in values.values()):
                    continue
                PosocoTableA.objects.update_or_create(
                    category=category,
                    report_date=today,
                    defaults={
                        'nr': values.get("NR"),
                        'wr': values.get("WR"),
                        'sr': values.get("SR"),
                        'er': values.get("ER"),
                        'ner': values.get("NER"),
                        'total': values.get("TOTAL"),
                    }
                )

        # Save data from Table G
        table_g_data = final_json.get("posoco_table_g", [])
        if table_g_data and table_g_data[0]:
            for fuel, values in table_g_data[0].items():
                if values is None or not isinstance(values, dict):
                    continue
                if all(v is None for v in values.values()):
                    continue
                PosocoTableG.objects.update_or_create(
                    fuel_type=fuel,
                    report_date=today,
                    defaults={
                        'nr': values.get("NR"),
                        'wr': values.get("WR"),
                        'sr': values.get("SR"),
                        'er': values.get("ER"),
                        'ner': values.get("NER"),
                        'all_india': values.get("All India"),
                        'share_percent': values.get("% Share"),
                    }
                )
        print("✅ Data saved to database successfully")
    except Exception as e:
        print(f"❌ An error occurred while saving to the database: {e}")

# --- Django Management Command ---
class Command(BaseCommand):
    help = "Downloads the latest NLDC PSP PDF, extracts key tables with shortened headings, and saves them to a file and the database."

    def add_arguments(self, parser):
        """
        Accept --date in YYYY-MM-DD or DD-MM-YYYY (or many other formats parsed by _parse_date_from_string).
        If omitted, the command will use today's date.
        """
        parser.add_argument(
            '--date',
            dest='date',
            required=False,
            help='Target report date to fetch (formats: YYYY-MM-DD, DD-MM-YYYY, DDMMYYYY, etc.). If omitted, uses today.'
        )

    def handle(self, *args, **options):
        self.stdout.write("🚀 Starting POSOCO report download and processing...")
        # Parse target date from --date if passed
        raw_date = options.get('date')
        if raw_date:
            parsed_dt = _parse_date_from_string(raw_date)
            if parsed_dt is None:
                self.stdout.write(self.style.ERROR(f"❌ Could not parse date passed: {raw_date}"))
                return
            target_date = parsed_dt.date() if isinstance(parsed_dt, datetime) else parsed_dt
        else:
            target_date = datetime.now().date()

        # Make report_dir including target_date so folder names reflect requested date
        report_dir, timestamp = make_report_dir(SAVE_DIR, desired_date=target_date)

        # Use the new fetch logic that fills with most-recent available by the requested day
        pdf_path, meta = fetch_report_for_target_date_with_fill(API_URL, BASE_URL, payload, report_dir, target_date, lookback_days=7)

        if pdf_path:
            # pass desired_date to extract_tables_from_pdf so JSON filename uses target_date
            final_json = extract_tables_from_pdf(pdf_path, report_dir, timestamp, desired_date=target_date)
            if final_json and (final_json["POSOCO"]["posoco_table_a"] or final_json["POSOCO"]["posoco_table_g"]):
                # Save to DB using the actual selected_report_date (meta) so database rows reflect the real report date
                selected_report_date = meta.get('selected_report_date') if meta else target_date
                save_to_db(final_json, report_date=selected_report_date)
                # Also print/save posting_date for auditing
                if meta and meta.get('selected_posting_date'):
                    print(f"ℹ️ Report posting date (when file was uploaded): {meta.get('selected_posting_date')}")
            else:
                self.stdout.write(self.style.WARNING("Could not extract any data from the PDF to save."))
        else:
            self.stdout.write(self.style.ERROR("Failed to download PDF. Aborting process."))

        self.stdout.write(self.style.SUCCESS("✅ Process finished."))