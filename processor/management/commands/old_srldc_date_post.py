#!/usr/bin/env python3
"""
Django management command to download SRLDC PSP PDFs, detect pattern (OLD/NEW),
extract Central Sector + Joint Venture tables, normalize rows (pattern-specific),
write JSON snapshot and save to DB (SRLDC3BData).

Drop this file into your app's management/commands/ directory and run like:
python manage.py srl_extract --date 2016-01-02 --debug
"""
import os
import re
import json
import datetime
import traceback
import requests
import pdfplumber
from datetime import datetime as _dt, timedelta
from decimal import Decimal, InvalidOperation

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import transaction

from processor.models import SRLDC3BData  # adjust if model path differs


# ----------------------------------------------------------------------
# ------------------ UTILITY / EXTRACTION FUNCTIONS --------------------
# ----------------------------------------------------------------------

def build_srl_url(date_obj):
    yyyy = date_obj.strftime("%Y")
    ddmmyyyy = date_obj.strftime("%d-%m-%Y")
    month_short = date_obj.strftime("%b")  # Dec
    year_short = date_obj.strftime("%y")  # 16
    monyy = f"{month_short}{year_short}"  # Dec16
    return f"https://srldc.in/var/ftp/reports/psp/{yyyy}/{monyy}/{ddmmyyyy}-psp.pdf"


def clean_cell(c):
    if c is None:
        return ""
    return re.sub(r"\s+", " ", str(c)).strip()


def first_cell_text(row):
    return clean_cell(row[0]).upper() if row and len(row) > 0 else ""


# --- MARKERS FOR OLD PATTERN ---
START_MARKER = r"CENTRAL\s+SECTOR"
JV_MARKER = r"JOINT\s+VENTURE"
END_MARKER = r"TOTAL\s+JV"


def is_start_row(text):
    return bool(re.search(START_MARKER, text, flags=re.IGNORECASE))


def is_jv_row(text):
    return bool(re.search(JV_MARKER, text, flags=re.IGNORECASE))


def is_end_row(text):
    return bool(re.search(END_MARKER, text, flags=re.IGNORECASE))


def looks_like_station_text(s):
    if not s:
        return False
    s = s.strip()
    # It should have some letters, and not be just numbers or symbols
    return bool(re.search(r"[A-Za-z]", s)) and not re.fullmatch(r"[-\d\.\,]+", s)


def parse_int_safe(s):
    s = (s or "").strip()
    if not s or s in ["--", "-"]:
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except:
        return None


def parse_float_safe(s):
    s = (s or "").strip()
    if not s or s in ["--", "-"]:
        return None
    try:
        return float(s.replace(",", ""))
    except:
        return None


def _try_parse_date_token(tok):
    if not tok:
        return None
    tok = str(tok).strip()
    tok_norm = re.sub(r"[,\s]+", " ", tok.replace("/", "-")).strip()
    fmts = ["%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%d-%b-%y", "%d-%m-%y"]
    for fmt in fmts:
        try:
            if "%b" in fmt or "%B" in fmt:
                dt = _dt.strptime(tok_norm.title(), fmt)
            else:
                dt = _dt.strptime(tok_norm, fmt)
            return dt.date()
        except Exception:
            continue
    m = re.search(r"([0-3]?\d)[^\dA-Z]+([A-Z]{3,9})[^\dA-Z]+(\d{4})", tok.upper())
    if m:
        day, mon, year = m.group(1), m.group(2).title(), m.group(3)
        try:
            for fmt in ("%b", "%B"):
                try:
                    mon_num = _dt.strptime(mon, fmt).month
                    return _dt(int(year), mon_num, int(day)).date()
                except Exception:
                    pass
        except Exception:
            pass
    return None


def _try_parse_datetime_tokens(date_tok, time_tok):
    if not date_tok:
        return None
    date_obj = _try_parse_date_token(date_tok)
    if not date_obj:
        return None
    if not time_tok:
        return datetime.datetime.combine(date_obj, datetime.time())
    time_str = re.sub(r"[^\d:]", "", str(time_tok))
    m = re.search(r"([0-2]?\d)[:\.]([0-5]\d)", time_str)
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2))
        try:
            return _dt(date_obj.year, date_obj.month, date_obj.day, hour, minute)
        except Exception:
            return None
    return None


# ----------------------------------------------------------------------
# --- OLD-PATTERN date extraction (returns both left/right) - unchanged
# ----------------------------------------------------------------------
def extract_report_dates_old(pdf_path):
    res = {
        "report_date_for": None,
        "report_date_of_reporting": None,
        "reporting_datetime_of_reporting": None,
        "report_date": None,
        "reporting_datetime": None
    }

    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        page_text = first_page.extract_text() or ""
        try:
            page_words = first_page.extract_words(use_text_flow=True)
        except Exception:
            page_words = first_page.extract_words() if hasattr(first_page, "extract_words") else []

    txt = page_text

    # tolerant FOR pattern
    for_pattern = re.compile(
        r"\bFOR\b[^\n]{0,120}?([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4}|[0-3]?\d[-/]\d{1,2}[-/]\d{4})",
        flags=re.IGNORECASE
    )
    for_matches = list(for_pattern.finditer(txt))
    if for_matches:
        m = for_matches[0]
        token = m.group(1)
        dt_for = _try_parse_date_token(token)
        if dt_for:
            res["report_date_for"] = dt_for.isoformat()
            res["report_date"] = dt_for.isoformat()

    if not res["report_date_for"]:
        date_token_re = re.compile(r"([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4}|[0-3]?\d[-/]\d{1,2}[-/]\d{4})",
                                   flags=re.IGNORECASE)
        candidates = list(date_token_re.finditer(txt))
        if candidates:
            for cand in candidates:
                dt_tok = cand.group(1)
                start = cand.start(1)
                context_start = max(0, start - 60)
                context = txt[context_start:start].lower()
                if " for " in (" " + context + " ") or context.strip().endswith("for") or "for the" in context:
                    dt_for = _try_parse_date_token(dt_tok)
                    if dt_for:
                        res["report_date_for"] = dt_for.isoformat()
                        res["report_date"] = dt_for.isoformat()
                        break

    m_explicit = re.search(
        r"DATE\s+OF\s+REPORTING\s*[:\-]?\s*([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4}|[0-3]?\d[-/]\d{1,2}[-/]\d{4}|[0-3]?\d[^\n]{0,20}\d{4})(?:\s*(?:AT|@)?\s*([0-2]?\d[:\.]?[0-5]?\d[^\s]*))?",
        txt, flags=re.IGNORECASE)
    explicit_date_obj = None
    explicit_dt = None
    if m_explicit:
        dt_token = m_explicit.group(1)
        explicit_date_obj = _try_parse_date_token(dt_token)
        time_tok = m_explicit.group(2) if m_explicit.lastindex and m_explicit.lastindex >= 2 else None
        if explicit_date_obj:
            res["report_date_of_reporting"] = explicit_date_obj.isoformat()
            if time_tok:
                explicit_dt = _try_parse_datetime_tokens(dt_token, time_tok)
                if explicit_dt:
                    res["reporting_datetime_of_reporting"] = explicit_dt.strftime("%Y-%m-%d %H:%M")
                    res["reporting_datetime"] = res["reporting_datetime_of_reporting"]

    if not explicit_date_obj and page_words:
        wlist = [(i, w.get("text", ""), w.get("x0", 0), w.get("x1", 0), w.get("top", 0), w) for i, w in
                 enumerate(page_words)]
        texts_lower = [w[1].lower() for w in wlist]

        for i in range(len(texts_lower)):
            if texts_lower[i] == "date":
                window = texts_lower[i:i + 8]
                if "reporting" in window:
                    rel = window.index("reporting")
                    reporting_idx = i + rel
                else:
                    continue
                label_word = wlist[reporting_idx]
                label_top = label_word[4]
                label_right = label_word[3]
                candidates = [w for w in wlist if abs(w[4] - label_top) < 12 and w[2] > label_right]
                candidates = sorted(candidates, key=lambda x: x[2])
                for cand in candidates[:10]:
                    ct = cand[1]
                    mdate = re.search(r"([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4})", ct, flags=re.IGNORECASE)
                    if not mdate:
                        mdate = re.search(r"([0-3]?\d[-/]\d{1,2}[-/]\d{4})", ct)
                    if mdate:
                        dt_tok = mdate.group(1)
                        explicit_date_obj = _try_parse_date_token(dt_tok)
                        for cand2 in candidates[:10]:
                            ct2 = cand2[1]
                            mtime = re.search(r"([0-2]?\d[:\.]([0-5]\d))", ct2)
                            if mtime:
                                explicit_dt = _try_parse_datetime_tokens(dt_tok, mtime.group(1))
                                break
                        break
                if explicit_date_obj:
                    res["report_date_of_reporting"] = explicit_date_obj.isoformat()
                    if explicit_dt:
                        res["reporting_datetime_of_reporting"] = explicit_dt.strftime("%Y-%m-%d %H:%M")
                        res["reporting_datetime"] = res["reporting_datetime_of_reporting"]
                    break

    if not res["report_date_of_reporting"]:
        m_generic = re.search(r"(DATE[^A-Z0-9\n]{0,10}OF[^A-Z0-9\n]{0,10}REPORTING|DATE[^A-Z0-9\n]{0,10}REPORTING)",
                              txt, flags=re.IGNORECASE)
        if m_generic:
            tail = txt[m_generic.end(): m_generic.end() + 200]
            mdate = re.search(r"([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4}|[0-3]?\d[-/]\d{1,2}[-/]\d{4})", tail,
                              flags=re.IGNORECASE)
            if mdate:
                dt_tok = mdate.group(1)
                explicit_date_obj = _try_parse_date_token(dt_tok)
                if explicit_date_obj:
                    res["report_date_of_reporting"] = explicit_date_obj.isoformat()
                    mtime = re.search(r"(?:AT|@)?\s*([0-2]?\d[:\.]([0-5]\d))", tail, flags=re.IGNORECASE)
                    if mtime:
                        explicit_dt = _try_parse_datetime_tokens(dt_tok, mtime.group(1))
                        if explicit_dt:
                            res["reporting_datetime_of_reporting"] = explicit_dt.strftime("%Y-%m-%d %H:%M")
                            res["reporting_datetime"] = res["reporting_datetime_of_reporting"]

    if not res["report_date"]:
        if res.get("report_date_for"):
            res["report_date"] = res["report_date_for"]
        elif res.get("report_date_of_reporting"):
            res["report_date"] = res["report_date_of_reporting"]

    return res


# ----------------------------------------------------------------------
# --- NEW-PATTERN date extraction (returns both left/right)
# ----------------------------------------------------------------------
def extract_report_dates_new(pdf_path):
    res = {
        "report_date_for": None,
        "report_date_of_reporting": None,
        "reporting_datetime_of_reporting": None,
        "report_date": None,
        "reporting_datetime": None
    }

    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        page_text = first_page.extract_text() or ""
        try:
            page_words = first_page.extract_words(use_text_flow=True)
        except Exception:
            page_words = first_page.extract_words() if hasattr(first_page, "extract_words") else []

    txt = page_text

    explicit_date_obj = None
    explicit_dt = None
    m_explicit = re.search(
        r"DATE\s+OF\s+REPORTING\s*[:\-]?\s*([0-3]?\d[^\n]{0,40}?\d{4})(?:\s*(?:AT|@)?\s*([0-2]?\d[:\.]?[0-5]?\d[^\s]*))?",
        txt, flags=re.IGNORECASE)
    if m_explicit:
        dt_token = m_explicit.group(1)
        explicit_date_obj = _try_parse_date_token(dt_token)
        time_tok = m_explicit.group(2) if m_explicit.lastindex and m_explicit.lastindex >= 2 else None
        if explicit_date_obj:
            res["report_date_of_reporting"] = explicit_date_obj.isoformat()
            res["reporting_datetime"] = None
            if time_tok:
                explicit_dt = _try_parse_datetime_tokens(dt_token, time_tok)
                if explicit_dt:
                    res["reporting_datetime_of_reporting"] = explicit_dt.strftime("%Y-%m-%d %H:%M")
                    res["reporting_datetime"] = res["reporting_datetime_of_reporting"]

    for_matches = list(re.finditer(r"\bFOR\s+(?:THE\s+DAY\s+ENDED\s+)?([0-3]?\d[^\n]{0,40}?\d{4})",
                                   txt, flags=re.IGNORECASE))
    if for_matches:
        m = for_matches[0]
        token = m.group(1)
        dt_for = _try_parse_date_token(token)
        if dt_for:
            res["report_date_for"] = dt_for.isoformat()
            res["report_date"] = dt_for.isoformat()

    if not explicit_date_obj and page_words:
        wlist = [(i, w.get("text", ""), float(w.get("x0", 0)), float(w.get("x1", 0)), float(w.get("top", 0)), w)
                 for i, w in enumerate(page_words)]
        texts_lower = [w[1].lower() for w in wlist]

        for idx in range(len(texts_lower)):
            if texts_lower[idx] == "date":
                window = texts_lower[idx: idx + 8]
                if "reporting" in window:
                    rel = window.index("reporting")
                    reporting_idx = idx + rel
                else:
                    continue
                label_word = wlist[reporting_idx]
                label_top = label_word[4]
                label_right = label_word[3]
                candidates = [w for w in wlist if abs(w[4] - label_top) < 12 and w[2] > label_right]
                candidates = sorted(candidates, key=lambda x: x[2])
                explicit_date_obj = None
                explicit_dt = None
                for cand in candidates[:12]:
                    ct = cand[1]
                    mdate = re.search(r"([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4})", ct, flags=re.IGNORECASE)
                    if not mdate:
                        mdate = re.search(r"([0-3]?\d[-/]\d{1,2}[-/]\d{4})", ct)
                    if mdate:
                        dt_tok = mdate.group(1)
                        explicit_date_obj = _try_parse_date_token(dt_tok)
                        for cand2 in candidates[:12]:
                            ct2 = cand2[1]
                            mtime = re.search(r"([0-2]?\d[:\.]([0-5]\d))", ct2)
                            if mtime:
                                explicit_dt = _try_parse_datetime_tokens(dt_tok, mtime.group(1))
                                break
                        break
                if explicit_date_obj:
                    res["report_date_of_reporting"] = explicit_date_obj.isoformat()
                    if explicit_dt:
                        res["reporting_datetime_of_reporting"] = explicit_dt.strftime("%Y-%m-%d %H:%M")
                        res["reporting_datetime"] = res["reporting_datetime_of_reporting"]
                    break

    if not res["report_date_for"]:
        m_for2 = re.search(
            r"(?:POWER\s+SUPPLY\s+POSITION[^\n]{0,80}FOR|FOR\s+THE\s+DAY|FOR\s+)[^\n]{0,80}([0-3]?\d[-/][A-Za-z]{3,9}[-/]\d{4}|[0-3]?\d[-/]\d{1,2}[-/]\d{4})",
            txt, flags=re.IGNORECASE)
        if m_for2:
            tok = m_for2.group(1)
            dt_for = _try_parse_date_token(tok)
            if dt_for:
                res["report_date_for"] = dt_for.isoformat()
                res["report_date"] = dt_for.isoformat()

    if not res["report_date"] and res["report_date_of_reporting"]:
        res["report_date"] = res["report_date_of_reporting"]

    return res


# ----------------------------------------------------------------------
# Entrypoint wrapper: try OLD first, if it returns nothing then try NEW
# ----------------------------------------------------------------------
def extract_report_dates(pdf_path):
    old_res = extract_report_dates_old(pdf_path)
    if old_res.get("report_date_for") or old_res.get("report_date_of_reporting"):
        return old_res
    new_res = extract_report_dates_new(pdf_path)
    return new_res


# ----------------------------------------------------------------------
# Logic for OLD PATTERN (Strict Regex Markers)
# ----------------------------------------------------------------------
def extract_two_tables(pdf_path, report_info):
    tables_dict = {"report_date": report_info.get('report_date'),
                   "reporting_datetime": report_info.get('reporting_datetime'),
                   "central_sector": [], "joint_venture": []}
    current_table = None
    finished = False

    with pdfplumber.open(pdf_path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for t_idx, table in enumerate(tables, start=1):
                rows = [[clean_cell(c) for c in r] for r in table]
                if not rows:
                    continue
                maxcols = max(len(r) for r in rows)
                rows = [r + [""] * (maxcols - len(r)) for r in rows]

                for r in rows:
                    first = first_cell_text(r)
                    if not current_table and is_start_row(first):
                        current_table = "central_sector"
                        continue
                    if current_table != "joint_venture" and is_jv_row(first):
                        current_table = "joint_venture"
                        continue
                    if not current_table:
                        continue
                    if is_end_row(first):
                        tables_dict[current_table].append((pno, t_idx, r))
                        finished = True
                        break
                    tables_dict[current_table].append((pno, t_idx, r))
                if finished:
                    return tables_dict
    return tables_dict


# ----------------------------------------------------------------------
# Logic for NEW PATTERN (Internal State Machine)
# ----------------------------------------------------------------------
def extract_tables_new_pattern(pdf_path):
    tables_dict = {"central_sector": [], "joint_venture": []}
    is_capturing = False
    current_section = "central_sector"

    with pdfplumber.open(pdf_path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            if pno < 2:
                continue
            tables = page.extract_tables() or []
            for t_idx, table in enumerate(tables, start=1):
                rows = [[clean_cell(c) for c in r] for r in table]
                if not rows:
                    continue
                maxcols = max(len(r) for r in rows)
                rows = [r + [""] * (maxcols - len(r)) for r in rows]
                for r in rows:
                    row_text_full = " ".join(r).upper()
                    row_text_nospace_upper = row_text_full.replace(" ", "").upper()
                    first_col = clean_cell(r[0]).upper()
                    first_col_nospace = clean_cell(r[0]).replace(" ", "").upper()

                    # Start triggers
                    if "REGIONAL" in row_text_full and "ENTITIES" in row_text_full and "GENERATION" in row_text_full:
                        is_capturing = True
                        current_section = "central_sector"
                        continue
                    if first_col == "ISGS":
                        is_capturing = True
                        current_section = "central_sector"
                        continue

                    # Switch triggers
                    if "JOINT VENTURE" in row_text_full or "JOINT_VENTURE" in row_text_full or (
                            "JOINT" in row_text_full and "VENTURE" in row_text_full):
                        is_capturing = True
                        current_section = "joint_venture"
                        other_cells = [clean_cell(c).strip() for c in r[1:]]
                        if all(not oc for oc in other_cells):
                            continue

                    # Stop
                    if "IPPUNDEROPENACCESS" in row_text_nospace_upper:
                        is_capturing = False
                        continue

                    # Data capture
                    if is_capturing:
                        if "STATION" in row_text_full and "CAPACITY" in row_text_full:
                            continue
                        if "INST." in row_text_full and "CAPACITY" in row_text_full:
                            continue
                        if first_col_nospace in ("JOINTVENTURE", "JOINT_VENTURE", "JOINTVENTURE:"):
                            other_cells = [clean_cell(c).strip() for c in r[1:]]
                            if all(not oc for oc in other_cells):
                                continue
                        if looks_like_station_text(r[0]):
                            tables_dict[current_section].append((pno, t_idx, r))

    return tables_dict


# ----------------------------------------------------------------------
# Debug helpers: detect pattern and dump raw tables
# ----------------------------------------------------------------------
def detect_table_pattern(tables_dict):
    """
    Decide pattern type based on header keywords found in the extracted text.
    Returns: "OLD", "NEW", or "UNKNOWN"
    """
    central = tables_dict.get("central_sector", [])
    if not central:
        return "UNKNOWN"

    # Join the text of the first few rows to look for keywords
    sample_rows = []
    for (p, t, r) in central[:6]:
        # Join all cells in the row into a single uppercase string
        row_text = " ".join([clean_cell(x).upper() for x in r if x is not None])
        sample_rows.append(row_text)

    full_text = "\n".join(sample_rows)

    # CHECK FOR NEW PATTERN SPECIFIC KEYWORDS
    # The new pattern has "Min Generation", "Gross Gen", or "Net Gen"
    if "MIN GENERATION" in full_text or "GROSS GEN" in full_text or "NET GET" in full_text:
        return "NEW"

    # CHECK FOR OLD PATTERN SPECIFIC BEHAVIOR
    # The old pattern just has "Day Energy" and usually lacks Min Gen headers
    if "DAY ENERGY" in full_text and "MIN GENERATION" not in full_text:
        return "OLD"

    # Fallback: Count columns in data rows
    # Old pattern usually has ~8 columns total
    # New pattern usually has ~11 columns total
    data_row_lengths = []
    for (p, t, r) in central:
        # filter out empty cells
        cleaned = [x for x in r if clean_cell(x)]
        if len(cleaned) > 5:  # only count meaningful rows
            data_row_lengths.append(len(cleaned))

    if data_row_lengths:
        avg_len = sum(data_row_lengths) / len(data_row_lengths)
        if avg_len >= 9:
            return "NEW"
        else:
            return "OLD"

    return "UNKNOWN"


def dump_raw_tables(pdf_path, out_folder):
    os.makedirs(out_folder, exist_ok=True)
    with pdfplumber.open(pdf_path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            js = []
            for t in tables:
                rows = [[clean_cell(c) for c in r] for r in t]
                js.append(rows)
            fname = os.path.join(out_folder, f"raw_page_{pno:03d}.json")
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(js, f, indent=2, ensure_ascii=False)


def null_if_empty(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None


# ----------------------------------------------------------------------
# NEW normalizer (Pattern 1 - Complex, Min Gen, Gross/Net)
# ----------------------------------------------------------------------
def normalize_rows_for_table_new(rows_with_meta, report_info=None):
    """
    Normalizer for NEW PATTERN (Image 1 - Complex).
    Columns after Station Name:
    0: Inst Capacity
    1: 19:00 Peak
    2: 03:00 Off Peak
    3: Day Peak MW
    4: Day Peak Hrs
    5: Min Generation MW  <-- Extra column
    6: Min Generation Hrs <-- Extra column
    7: Gross Gen (MU)     <-- Energy Split
    8: Net Gen (MU)       <-- Energy Split
    9: Avg MW
    """
    report_info = report_info or {}
    recs = []

    for page, tbl_idx, r in rows_with_meta:
        station_index = None
        for i, c in enumerate(r):
            if looks_like_station_text(c):
                station_index = i
                break
        if station_index is None:
            continue

        station = clean_cell(r[station_index]).strip()
        tail = r[station_index + 1:]
        tail_clean = [clean_cell(x) for x in tail]

        def safe_val(idx, func):
            if idx < len(tail_clean):
                return func(tail_clean[idx])
            return None

        # --- MAPPING FOR NEW PATTERN ---
        installed_capacity = safe_val(0, parse_int_safe)
        peak_1900 = safe_val(1, parse_int_safe)
        offpeak_0300 = safe_val(2, parse_int_safe)
        day_peak_mw = safe_val(3, parse_int_safe)
        day_peak_hrs = safe_val(4, lambda x: x)

        # New Pattern Specifics
        min_generation_mw = safe_val(5, parse_float_safe)
        min_generation_hrs = safe_val(6, lambda x: x)
        gross_energy_mu = safe_val(7, parse_float_safe)
        net_energy_mu = safe_val(8, parse_float_safe)

        # Avg MW is at the end (index 9)
        avg_mw = safe_val(9, parse_float_safe)

        # In New Pattern, 'Day Energy' is split into Gross/Net.
        # We can leave day_energy_mu as None, or equal to Gross.
        day_energy_mu = None

        row_type = "TOTAL" if station.upper().startswith("TOTAL") else "GENERATOR"

        recs.append({
            "station": station,
            "installed_capacity_mw": installed_capacity,
            "peak_1900_mw": peak_1900,
            "offpeak_0300_mw": offpeak_0300,
            "day_peak_mw": day_peak_mw,
            "day_peak_hrs": day_peak_hrs,
            "min_generation_mw": min_generation_mw,
            "min_generation_hrs": min_generation_hrs,
            "day_energy_mu": day_energy_mu,
            "gross_energy_mu": gross_energy_mu,
            "net_energy_mu": net_energy_mu,
            "avg_mw": avg_mw,
            "row_type": row_type,
            "source_page": page,
            "source_table_index": tbl_idx,
        })

    return recs


# ----------------------------------------------------------------------
# OLD normalizer (Pattern 2 - Simple, No Min Gen)
# ----------------------------------------------------------------------
def normalize_rows_for_table_old(rows_with_meta, report_info=None):
    """
    Normalizer for OLD PATTERN (Image 2 - Simple).
    Columns after Station Name:
    0: Inst Capacity
    1: 19:00 Peak
    2: 03:00 Off Peak
    3: Day Peak MW
    4: Day Peak Hrs
    5: Day Energy (MU)  <-- Value is here immediately after Peak Hrs
    6: Avg MW
    """
    report_info = report_info or {}
    recs = []

    for page, tbl_idx, r in rows_with_meta:
        station_index = None
        for i, c in enumerate(r):
            if looks_like_station_text(c):
                station_index = i
                break
        if station_index is None:
            continue

        station = clean_cell(r[station_index]).strip()
        tail = r[station_index + 1:]
        tail_clean = [clean_cell(x) for x in tail]

        def safe_val(idx, func):
            if idx < len(tail_clean):
                return func(tail_clean[idx])
            return None

        # --- MAPPING FOR OLD PATTERN ---
        installed_capacity = safe_val(0, parse_int_safe)
        peak_1900 = safe_val(1, parse_int_safe)
        offpeak_0300 = safe_val(2, parse_int_safe)
        day_peak_mw = safe_val(3, parse_int_safe)
        day_peak_hrs = safe_val(4, lambda x: x)

        # In Old Pattern, Index 5 is Day Energy
        day_energy_mu = safe_val(5, parse_float_safe)

        # In Old Pattern, Index 6 is Avg MW
        avg_mw = safe_val(6, parse_float_safe)

        # These fields do not exist in the Old Pattern
        min_generation_mw = None
        min_generation_hrs = None
        gross_energy_mu = None  # Or you can map day_energy_mu here if you prefer
        net_energy_mu = None

        row_type = "TOTAL" if station.upper().startswith("TOTAL") else "GENERATOR"

        recs.append({
            "station": station,
            "installed_capacity_mw": installed_capacity,
            "peak_1900_mw": peak_1900,
            "offpeak_0300_mw": offpeak_0300,
            "day_peak_mw": day_peak_mw,
            "day_peak_hrs": day_peak_hrs,
            "min_generation_mw": min_generation_mw,
            "min_generation_hrs": min_generation_hrs,
            "day_energy_mu": day_energy_mu,
            "gross_energy_mu": gross_energy_mu,
            "net_energy_mu": net_energy_mu,
            "avg_mw": avg_mw,
            "row_type": row_type,
            "source_page": page,
            "source_table_index": tbl_idx,
        })

    return recs


# ----------------------------------------------------------------------
# ------------------------- MAIN COMMAND -------------------------------
# ----------------------------------------------------------------------
class Command(BaseCommand):
    help = "Download SRLDC PDFs, extract CENTRAL SECTOR / JV tables, save JSON snapshot and save to DB (with pattern detection)"

    def add_arguments(self, parser):
        parser.add_argument("--date", help="Single date YYYY-MM-DD")
        parser.add_argument("--start", help="Start date YYYY-MM-DD")
        parser.add_argument("--end", help="End date YYYY-MM-DD")
        parser.add_argument("--debug", action="store_true", help="Dump raw tables and extra debug files")

    def log(self, msg, level="info"):
        ts = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
        full = f"[{ts}] {msg}"
        if level == "success":
            self.stdout.write(self.style.SUCCESS(full))
        elif level == "warning":
            self.stdout.write(self.style.WARNING(full))
        elif level == "error":
            self.stdout.write(self.style.ERROR(full))
        else:
            self.stdout.write(full)

    def handle(self, *args, **options):

        if options.get("date"):
            try:
                dates = [datetime.datetime.strptime(options["date"], "%Y-%m-%d").date()]
            except:
                raise CommandError("Invalid --date format")
        elif options.get("start") and options.get("end"):
            start = datetime.datetime.strptime(options["start"], "%Y-%m-%d").date()
            end = datetime.datetime.strptime(options["end"], "%Y-%m-%d").date()
            if start > end:
                raise CommandError("start date must be <= end date")
            dates = []
            d = start
            while d <= end:
                dates.append(d)
                d += timedelta(days=1)
        else:
            raise CommandError("Provide either --date or --start and --end")

        OUT_DIR = os.path.join(settings.BASE_DIR, "srl_on_json")
        os.makedirs(OUT_DIR, exist_ok=True)

        for d in dates:
            try:
                timestamp = _dt.now().strftime("%Y%m%d_%H%M%S")
                folder_name = f"{d.strftime('%Y%m%d')}_{timestamp}"
                download_folder = os.path.join(OUT_DIR, folder_name)
                os.makedirs(download_folder, exist_ok=True)

                url = build_srl_url(d)
                self.log("\n==============================", "info")
                self.log(f"Processing Date: {d}", "info")
                self.log("==============================", "info")

                # ---------------- DOWNLOAD -----------------
                self.log(f"[1] Downloading PDF from: {url}", "info")
                pdf_filename = f"{d.strftime('%d-%m-%Y')}.pdf"
                pdf_path = os.path.join(download_folder, pdf_filename)

                try:
                    resp = requests.get(url, verify=False, timeout=60)
                except Exception as e:
                    self.log(f"Download error for date {d}: {e}", "error")
                    raise

                if resp.status_code != 200:
                    self.log(f"Failed download for date {d} (status {resp.status_code})", "error")
                    raise Exception(f"HTTP {resp.status_code}")

                with open(pdf_path, "wb") as f:
                    f.write(resp.content)

                self.log(f"PDF saved: {pdf_path}", "success")

                # ---------------- EXTRACT ----------------
                self.log("[2] Extracting report dates...", "info")
                report_info = extract_report_dates(pdf_path)
                self.log(f"Report DATE found: {report_info.get('report_date')}", "info")
                self.log(f"Reporting DATETIME found: {report_info.get('reporting_datetime')}", "info")

                # --- Extract both OLD and NEW candidates ---
                self.log("[3] Extracting tables (OLD and NEW candidates)...", "info")
                old_tables = extract_two_tables(pdf_path, report_info)
                new_tables = extract_tables_new_pattern(pdf_path)

                # Detect pattern
                detected_pattern = detect_table_pattern(old_tables)
                if detected_pattern == "UNKNOWN":
                    detected_pattern = detect_table_pattern(new_tables)

                # Default to finding tables in OLD logic first, then NEW logic
                tables = old_tables
                if not tables.get("central_sector") and new_tables.get("central_sector"):
                    tables = new_tables

                self.log(f"Detected Pattern: {detected_pattern}", "info")

                central = []
                jv = []

                if detected_pattern == "NEW":
                    self.log("Using NEW pattern normalizer.", "info")
                    # Make sure we are using the tables extracted via NEW logic if available
                    if new_tables.get("central_sector"):
                        tables = new_tables

                    central = normalize_rows_for_table_new(tables.get("central_sector", []), report_info)
                    jv = normalize_rows_for_table_new(tables.get("joint_venture", []), report_info)

                else:
                    # Default to OLD pattern if detected is OLD or UNKNOWN (fallback)
                    self.log("Using OLD pattern normalizer.", "info")
                    central = normalize_rows_for_table_old(tables.get("central_sector", []), report_info)
                    jv = normalize_rows_for_table_old(tables.get("joint_venture", []), report_info)

                combined = (central or []) + (jv or [])

                self.log(f"Central Sector Rows: {len(central)}", "info")
                self.log(f"Joint Venture Rows: {len(jv)}", "info")
                self.log(f"TOTAL Extracted Rows: {len(combined)}", "success")

                # ---------------- JSON SNAPSHOT ----------------
                fname = f"{d.strftime('%Y%m%d')}.json"
                json_path = os.path.join(download_folder, fname)
                try:
                    snapshot = {
                        "report_date": report_info.get("report_date"),
                        "reporting_datetime": report_info.get("reporting_datetime"),
                        "central_sector": central,
                        "joint_venture": jv
                    }
                    with open(json_path, "w", encoding="utf-8") as jf:
                        json.dump(snapshot, jf, indent=2, ensure_ascii=False)

                    size = os.path.getsize(json_path)
                    self.log(f"JSON saved: {json_path} ({size} bytes)", "success")

                except Exception as e:
                    self.log(f"JSON write error for date {d}: {e}", "error")
                    raise

                # ---------------- SAVE TO DATABASE -------------
                self.log("[4] Saving rows to Database...", "info")
                saved = 0

                def to_dec(v):
                    if v is None:
                        return None
                    try:
                        return Decimal(str(v))
                    except (InvalidOperation, ValueError, TypeError):
                        return None

                report_date_parsed = None
                if report_info.get("report_date"):
                    report_date_parsed = _try_parse_date_token(report_info.get("report_date"))
                reporting_dt_parsed = None
                if report_info.get("reporting_datetime"):
                    try:
                        reporting_dt_parsed = _dt.strptime(report_info.get("reporting_datetime"), "%Y-%m-%d %H:%M")
                    except Exception:
                        reporting_dt_parsed = None

                with transaction.atomic():
                    for rec in combined:
                        report_date = report_date_parsed
                        obj, created = SRLDC3BData.objects.update_or_create(
                            station=rec.get("station"),
                            report_date=report_date,
                            defaults={
                                "reporting_datetime": reporting_dt_parsed,
                                "installed_capacity_mw": rec.get("installed_capacity_mw"),
                                "peak_1900_mw": rec.get("peak_1900_mw"),
                                "offpeak_0300_mw": rec.get("offpeak_0300_mw"),
                                "day_peak_mw": rec.get("day_peak_mw"),
                                "day_peak_hrs": rec.get("day_peak_hrs"),
                                "min_generation_mw": to_dec(rec.get("min_generation_mw")),
                                "min_generation_hrs": rec.get("min_generation_hrs"),
                                "gross_energy_mu": to_dec(rec.get("gross_energy_mu")),
                                "net_energy_mu": to_dec(rec.get("net_energy_mu")),
                                "day_energy_mu": to_dec(rec.get("day_energy_mu")),
                                "avg_mw": to_dec(rec.get("avg_mw")),
                                "row_type": rec.get("row_type"),
                                "source_page": rec.get("source_page"),
                                "source_table_index": rec.get("source_table_index"),
                            }
                        )
                        saved += 1

                self.log(f"Saved {saved} rows to DB for {d}", "success")

            except Exception as e:
                tb = traceback.format_exc()
                self.log(f"PROCESS FAILED for date {d}: {e}", "error")
                self.log(tb, "error")
                continue

        self.log("All dates processed.", "success")