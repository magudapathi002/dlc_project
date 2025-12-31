#!/usr/bin/env python3
import logging
import os
import re
import json
import datetime
import traceback
import requests
import pdfplumber
from datetime import datetime as _dt, timedelta
from decimal import Decimal, InvalidOperation

import pandas as pd
from tabula.io import read_pdf

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import transaction
from django.utils import timezone

# ---- Models: ensure these names match your app models ----
from processor.models import Srldc2AData, Srldc2CData, SRLDC3BData
# ---- SSL adapter used by your tabula downloader session (keeps legacy support) ----
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.poolmanager import PoolManager


class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        # Enable "Legacy Server Connect" (0x4) to allow unsafe renegotiation
        ctx.options |= 0x4
        # (Optional) Lower security level to allow older ciphers often used by gov sites
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


# ----------------- Shared helpers -----------------
def clean_cell(c):
    if c is None:
        return ""
    return re.sub(r"\s+", " ", str(c)).strip()


def first_cell_text(row):
    return clean_cell(row[0]).upper() if row and len(row) > 0 else ""


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


# ---------------- Tabula-based 2A/2C extractor (from your original script) ----------------
class TabulaExtractor:
    SOUTH_INDIAN_STATES = [
        "ANDHRA PRADESH", "KARNATAKA", "KERALA", "PONDICHERRY", "TAMILNADU", "TELANGANA", "REGION"
    ]
    SOUTH_INDIAN_STATES_2C = [
        "AP", "KAR", "KER", "PONDY", "TN", "TG", "REGION"
    ]

    def __init__(self, write_fn, logger):
        self.write = write_fn
        self.logger = logger

    def _safe_float(self, value):
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if ':' in value:
                return None
            value = value.replace(',', '')
            if not value or value.lower() in ['n/a', '-', 'null', 'nan', 'na', '']:
                return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_string(self, value):
        if pd.isna(value) or value is None:
            return None
        s_val = str(value).strip()
        if s_val.lower() == 'nan' or s_val == '':
            return None
        s_val = s_val.replace('\r', ' ')
        return s_val

    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0,
                                    debug_table_name="Unknown Table"):
        start_idx = None
        end_idx = None
        new_columns = None

        for i, row in df.iterrows():
            row_str_series = row.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
            if row_str_series.str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.write(self.style_warning(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}."),
                       level='warning')
            return None, None

        if end_marker:
            for i in range(start_idx + 1, len(df)):
                row_str_series = df.iloc[i].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                if row_str_series.str.contains(end_marker, regex=True, na=False, case=False).any():
                    end_idx = i
                    break

        if end_idx is not None:
            raw_sub_df = df.iloc[start_idx:end_idx].copy().reset_index(drop=True)
        else:
            raw_sub_df = df.iloc[start_idx:].copy().reset_index(drop=True)

        data_start_row_in_raw_sub_df = 1 + header_row_count

        if header_row_count > 0 and len(raw_sub_df) >= data_start_row_in_raw_sub_df:
            headers_df = raw_sub_df.iloc[1: data_start_row_in_raw_sub_df]
            new_columns = []
            if debug_table_name == "Table 2(A)":
                new_columns = [
                    'STATE',
                    'THERMAL',
                    'HYDRO',
                    'GAS/DIESEL/NAPTHA',
                    'WIND',
                    'SOLAR',
                    'OTHERS',
                    'Net SCH (Net Mu)',
                    'Drawal (Net Mu)',
                    'UI (Net Mu)',
                    'Availability (Net MU)',
                    'Demand Met (Net MU)',
                    'Shortage # (Net MU)'
                ]
            elif debug_table_name == "Table 2(C)":
                new_columns = [
                    'State',
                    'Maximum Demand Met of the day',
                    'Time',
                    'Shortage during maximum demand',
                    'Requirement at maximum demand',
                    'Maximum requirement of the day',
                    'Time.1',
                    'Shortage during maximum requirement',
                    'Demand Met at maximum Requirement',
                    'Min Demand Met',
                    'Time.2',
                    'ACE_MAX',
                    'Time.3',
                ]
            else:
                # generic fallback combining two-row header
                raw_top_header = headers_df.iloc[0].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna(
                    '')
                raw_bottom_header = headers_df.iloc[1].astype(str).str.replace('\n', ' ',
                                                                               regex=False).str.strip().fillna('')
                for idx in range(raw_top_header.shape[0]):
                    t_col = raw_top_header.iloc[idx].strip()
                    b_col = raw_bottom_header.iloc[idx].strip()
                    if not t_col and not b_col:
                        new_columns.append(f"Unnamed_{idx}")
                    elif not b_col:
                        new_columns.append(t_col)
                    elif not t_col:
                        new_columns.append(b_col)
                    elif not b_col.startswith(t_col):
                        new_columns.append(f"{t_col} {b_col}".strip())
                    else:
                        new_columns.append(b_col)

            if new_columns is not None:
                sub_df_data = raw_sub_df.iloc[data_start_row_in_raw_sub_df:].copy()
                sub_df_data = sub_df_data.reindex(
                    columns=list(sub_df_data.columns) + [col for col in new_columns if col not in sub_df_data.columns])
                sub_df_data = sub_df_data.iloc[:, :len(new_columns)]
                sub_df_data.columns = new_columns
                sub_df_data = sub_df_data.loc[:, ~sub_df_data.columns.duplicated(keep='first')]
                sub_df_data.columns = sub_df_data.columns.astype(str).str.strip()
                sub_df_data.columns = sub_df_data.columns.str.replace(r'\s*\r\s*', ' ', regex=True).str.strip()
                sub_df_data = sub_df_data.dropna(axis=0, how='all')
                return sub_df_data.dropna(axis=1, how='all'), new_columns
            else:
                return raw_sub_df.iloc[1:].dropna(axis=1, how='all'), None
        else:
            return raw_sub_df.iloc[1:].dropna(axis=1, how='all'), None

    # small helper to adapt style methods used above
    def style_warning(self, msg):
        return msg


# ---------------- pdfplumber-based 3B extractor (from your 3B script) ----------------
# Markers used by 3B logic
START_MARKER_3B = r"CENTRAL\s+SECTOR"
JV_MARKER_3B = r"JOINT\s+VENTURE"
END_MARKER_3B = r"TOTAL\s+JV"


def is_start_row_3b(text): return bool(re.search(START_MARKER_3B, text, flags=re.IGNORECASE))


def is_jv_row_3b(text): return bool(re.search(JV_MARKER_3B, text, flags=re.IGNORECASE))


def is_end_row_3b(text): return bool(re.search(END_MARKER_3B, text, flags=re.IGNORECASE))


# Date extraction helpers (both old/new pattern) - copied from your 3B script
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

    for_pattern = re.compile(
        r"FOR(\d{1,2}[-/][A-Za-z]{3,9}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})",
        flags=re.IGNORECASE
    )

    for_matches = list(for_pattern.finditer(txt))
    if for_matches:
        m = for_matches[0]
        token = m.group(1)
        dt_for = _try_parse_date_token(token)
        if dt_for:
            # res["report_date_for"] = dt_for.isoformat()
            res["report_date"] = dt_for.isoformat()
            print(dt_for, "qwertyuiop")

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
                explicit_date_obj = None
                explicit_dt = None
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

    m_explicit = re.search(
        r"DATE\s+OF\s+REPORTING\s*[:\-]?\s*([0-3]?\d[^\n]{0,40}?\d{4})(?:\s*(?:AT|@)?\s*([0-2]?\d[:\.]?[0-5]?\d[^\s]*))?",
        txt, flags=re.IGNORECASE)
    explicit_date_obj = None
    explicit_dt = None
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

        for_matches = re.compile(
            r"FOR(\d{1,2}[-/][A-Za-z]{3,9}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})",
            flags=re.IGNORECASE
        )

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


def extract_report_dates(pdf_path):
    old_res = extract_report_dates_old(pdf_path)
    if old_res.get("report_date_for") or old_res.get("report_date_of_reporting"):
        return old_res
    new_res = extract_report_dates_new(pdf_path)
    return new_res


# 3B row extraction (OLD/NEW pattern scanning)
def extract_two_tables_3b(pdf_path, report_info):
    tables_dict = {"report_date": report_info.get('report_date'),
                   "reporting_datetime": report_info.get('reporting_datetime'), "central_sector": [],
                   "joint_venture": []}
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
                    if not current_table and is_start_row_3b(first):
                        current_table = "central_sector"
                        continue
                    if current_table != "joint_venture" and is_jv_row_3b(first):
                        current_table = "joint_venture"
                        continue
                    if not current_table:
                        continue
                    if is_end_row_3b(first):
                        tables_dict[current_table].append((pno, t_idx, r))
                        finished = True
                        break
                    tables_dict[current_table].append((pno, t_idx, r))
                if finished:
                    return tables_dict
    return tables_dict


def extract_tables_new_pattern_3b(pdf_path):
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

                    if "REGIONAL" in row_text_full and "ENTITIES" in row_text_full and "GENERATION" in row_text_full:
                        is_capturing = True
                        current_section = "central_sector"
                        continue
                    if first_col == "ISGS":
                        is_capturing = True
                        current_section = "central_sector"
                        continue
                    if "JOINT VENTURE" in row_text_full or "JOINT_VENTURE" in row_text_full or (
                            "JOINT" in row_text_full and "VENTURE" in row_text_full):
                        is_capturing = True
                        current_section = "joint_venture"
                        other_cells = [clean_cell(c).strip() for c in r[1:]]
                        if all(not oc for oc in other_cells):
                            continue
                    if "IPPUNDEROPENACCESS" in row_text_nospace_upper:
                        is_capturing = False
                        continue
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




def normalize_rows_for_table_3b(rows_with_meta, report_info=None):
    """
    Extracts all columns from SRLDC 3(B) including:
    - Installed, Peak, Offpeak
    - Day Peak MW + Hrs
    - Min Generation MW + Hrs  (if present)
    - Day Energy: Gross Gen (MU), Net Gen (MU)
    - Avg MW

    IMPORTANT:
    For TOTAL rows where Min Generation columns are blank, the numbers after
    index 5 belong to Day Energy (Gross, Net, Avg). We detect this and avoid
    mis-mapping them into min_generation_mw.
    """
    report_info = report_info or {}
    recs = []

    for page, tbl_idx, r in rows_with_meta:

        # -------- FIND STATION NAME ----------
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

        def safe_int(idx):
            return parse_int_safe(tail_clean[idx]) if idx < len(tail_clean) else None

        def safe_str(idx):
            return tail_clean[idx] if idx < len(tail_clean) else ""

        # ---- FIXED POSITIONS ----
        installed_capacity = safe_int(0)
        peak_1900 = safe_int(1)
        offpeak_0300 = safe_int(2)
        day_peak_mw = safe_int(3)
        day_peak_hrs = safe_str(4)

        # ---- TRY TO FIND MIN GENERATION (MW + HRS) ----
        min_generation_mw = None
        min_generation_hrs = None

        idx = 5  # start looking after day_peak_hrs

        # first numeric candidate = min generation MW
        while idx < len(tail_clean):
            # Special handling for "0" which is valid logic but sometimes confusing if followed by "-"
            v = parse_float_safe(tail_clean[idx])
            if v is not None:
                min_generation_mw = v
                idx += 1
                
                # Check if next token is valid time OR placeholder ("-" or empty)
                if idx < len(tail_clean):
                     tok = tail_clean[idx]
                     if tok == '-' or tok == '' or tok == '0': # Treat 0 as valid placeholder for Time if needed?
                         min_generation_hrs = None
                         idx += 1
                         # Don't break, let the next loop confirm? 
                         # Actually we should break because we found the "Time" slot (it was empty/-)
                         break
                break
            idx += 1

        # next time-like token = min generation Hrs
        # Only search if we haven't consumed it as a placeholder above
        # pattern check
        if min_generation_mw is not None and min_generation_hrs is None and idx < len(tail_clean):
             tok = tail_clean[idx]
             if re.match(r"^\d{1,2}:\d{2}$", tok) or re.match(r"^\d{1,2}$", tok) or tok=='-':
                 if tok != '-': min_generation_hrs = tok
                 idx += 1

        # ---------- DECIDE WHERE DAY ENERGY STARTS ----------
        # If we did NOT find both MW and Hrs for Min Gen, assume this row
        # has *no* Min Gen block (typical TOTAL rows) and roll back.
        # FIX: valid if we found MW (even 0) and explicitly handled Hrs (found or placeholder)
        if min_generation_mw is None: # Only reset if we didn't find MW at all
             energy_start_idx = 5
             min_generation_mw = None
             min_generation_hrs = None
        else:
             energy_start_idx = idx

        # ---------- EXTRACT GROSS, NET, AVG ----------
        remaining = tail_clean[energy_start_idx:]

        numeric_values = []
        for cell in remaining:
            num = parse_float_safe(cell)
            if num is not None:
                numeric_values.append(num)

        gross_energy_mu = None
        net_energy_mu = None
        avg_mw = None

        # EXPECTED ORDER: [Gross MU, Net MU, Avg MW]
        if len(numeric_values) >= 3:
            gross_energy_mu = numeric_values[0]
            net_energy_mu = numeric_values[1]
            avg_mw = numeric_values[2]
        elif len(numeric_values) == 2:
            gross_energy_mu = numeric_values[0]
            net_energy_mu = numeric_values[1]
        elif len(numeric_values) == 1:
            net_energy_mu = numeric_values[0]

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
            "gross_energy_mu": gross_energy_mu,
            "net_energy_mu": net_energy_mu,
            "avg_mw": avg_mw,
            "row_type": row_type,
            "source_page": page,
            "source_table_index": tbl_idx,
        })

    return recs




# ======================================================
# TABLE 2(A) EXTRACTION USING HEADING ANCHOR (WORKING)
# ======================================================
def extract_table_2A_using_heading(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        words = page.extract_words(use_text_flow=True)

        # Log words for debugging
        try:
             with open("d:\\dlc_project\\debug_extraction.log", "a", encoding="utf-8") as dbg:
                 dbg.write(f"\n--- Checking 2A in {pdf_path} ---\n")
                 dbg.write(f"First 50 words: {[w['text'] for w in words[:50]]}\n")
        except: pass

        # ---- FIND HEADING POSITION ----
        heading_top = None
        for w in words:
            txt = w.get("text", "").replace(" ", "").upper()
            if "2(A)" in txt or "2A" in txt:
                heading_top = w["top"]
                break
        
        if heading_top is None:
            try:
                with open("d:\\dlc_project\\debug_extraction.log", "a", encoding="utf-8") as dbg:
                    dbg.write(f"FAIL 2A. Heading not found.\n")
            except: pass
            print(f"DEBUG: 2(A) Heading not found. First 20 words: {[w['text'] for w in words[:20]]}")
            return None

        print(f"📍 Table 2(A) heading TOP position: {heading_top}")

        # ---- GET RELEVANT TABLE BY CONTENT SCAN ----
        for table in page.find_tables():
            # Table must end below the heading
            if table.bbox[3] > heading_top:
                df = pd.DataFrame(table.extract())
                # Scan for 2(A) headers: THERMAL, HYDRO, SOLAR
                header_idx = -1
                for idx, row in df.iterrows():
                    row_str = " ".join([str(x).upper() for x in row if x])
                    if "THERMAL" in row_str and "HYDRO" in row_str and "SOLAR" in row_str:
                        header_idx = idx
                        break
                
                if header_idx != -1:
                    # Found header. Try to include the row above if it has 'STATE'
                    start_slice = header_idx
                    if header_idx > 0:
                        prev_row = df.iloc[header_idx - 1].fillna("").astype(str).str.upper()
                        prev_str = " ".join(prev_row)
                        if "STATE" in prev_str:
                            start_slice = header_idx - 1
                    
                    return df.iloc[start_slice:].reset_index(drop=True)
        return None

    return None

# ======================================================
# TABLE 2(C) EXTRACTION USING HEADING ANCHOR (NEW)
# ======================================================
def extract_table_2C_using_heading(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]

        # ---- FIND HEADING POSITION ----
        heading_top = None
        words = page.extract_words(use_text_flow=True)
        for w in words:
            txt = w.get("text", "").replace(" ", "").upper()
            if "2(C)" in txt or "2C" in txt:
                heading_top = w["top"]
                break

        if heading_top is None:
            print(f"DEBUG: 2(C) Heading not found. First 20 words: {[w['text'] for w in words[:20]]}")
            return None

        print(f"📍 Table 2(C) heading TOP position: {heading_top}")

        # ---- GET RELEVANT TABLE BY CONTENT SCAN ----
        # ---- GET RELEVANT TABLE BY CONTENT SCAN ----
        for table in page.find_tables():
            # If table *ends* above heading, skip it completely
            if table.bbox[3] < heading_top:
                continue

            # Check if table *starts* significantly below heading (Standard case)
            # OR if it's a giant table overlapping the heading.
            # We will iterate ROWS to filter valid ones.
            
            # SIMPLER APPROACH for 2(C):
            # Extract distinct rows from the table object and filter by BBOX
            
            valid_rows_data = []
            for row in table.rows:
                # Check if row is strictly below heading
                # ROW bbox: (x0, top, x1, bottom)
                if row.bbox[1] > (heading_top + 2): # Small buffer
                    # Extract text from cells
                    row_data = [cell if cell else "" for cell in row.cells]
                    # Note: row.cells returns list of strings/None usually? 
                    # Actually pdfplumber Table.rows yields Row objects, and row.cells yields Cell objects (Rects)?
                    # No, table.extract() gives strings. table.rows gives Row objects.
                    # We might need to map index or re-extract?
                    # Let's rely on content filtering if we are in the same giant table since 2(A) is ABOVE 2(C).
                    pass

            # FALLBACK to content filtering on the whole extracted table, knowing 2(A) is atop 2(C)
            df = pd.DataFrame(table.extract())
            
            # Scan for 2(C) Header
            # We know 2(C) header has "MAXIMUM DEMAND MET" and "STATE"
            # 2(A) header has "STATE", "THERMAL", "HYDRO"
            
            header_idx = -1
            
            for idx, row in df.iterrows():
                row_str = " ".join([str(x).upper() for x in row if x])
                
                # Check for 2(A) signatures to skip
                if "THERMAL" in row_str and "HYDRO" in row_str and "NET SCH" in row_str:
                    continue
                if "THERMAL" in row_str and "HYDRO" in row_str:
                    continue    
                
                # Check for 2(C) signatures
                # Must NOT contain THERMAL, HYDRO (signatures of 2A)
                if "THERMAL" in row_str or "HYDRO" in row_str:
                    continue

                if ("MAXIMUM" in row_str and "DEMAND" in row_str and "MET" in row_str) or \
                   ("DEMAND" in row_str and "MET" in row_str and "ACE" in row_str) or \
                   ("ACE" in row_str and "MAX" in row_str and "MIN" in row_str):
                     # Found it!
                     header_idx = idx
                     print(f"DEBUG: Found 2(C) header at index {idx}: {row_str[:50]}...")
                     break
            
            if header_idx != -1:
                # Found header. Try to include the row above if it has 'STATE'
                start_slice = header_idx
                if header_idx > 0:
                    prev_row = df.iloc[header_idx - 1].fillna("").astype(str).str.upper()
                    prev_str = " ".join(prev_row)
                    # If prev row has "STATE", include it
                    if "STATE" in prev_str and "THERMAL" not in prev_str:
                        start_slice = header_idx - 1
                
                return df.iloc[start_slice:].reset_index(drop=True)

        return None


# ======================================================
# TABLE 3(B) EXTRACTION USING HEADING ANCHOR (NEW)
# ======================================================
def extract_table_3B_using_heading(pdf_path):
    tables_dict = {"central_sector": [], "joint_venture": []}
    
    current_section = None 
    start_collecting = False

    with pdfplumber.open(pdf_path) as pdf:
        # 1. FIND HEADING "3(B)"
        heading_page_idx = -1
        heading_top = -1
        
        for p_idx, page in enumerate(pdf.pages):
            for w in page.extract_words(use_text_flow=True):
                if "3(B)" in w.get("text", "").upper():
                    heading_page_idx = p_idx
                    heading_top = w["top"]
                    break
            if heading_page_idx != -1: break
            
        if heading_page_idx == -1:
            print("❌ Table 3(B) heading NOT FOUND.")
            return tables_dict

        print(f"📍 Table 3(B) heading FOUND on Page {heading_page_idx+1} at top={heading_top}")

        # 2. EXTRACT TABLES
        for p_idx in range(heading_page_idx, len(pdf.pages)):
            page = pdf.pages[p_idx]
            found_tables = page.find_tables()
            
            for t_obj in found_tables:
                # FIX: Check if table *ends* above heading. If so, skip.
                # Do NOT skip if table starts above (bbox[1]) but ends below (overlapping/giant table)
                if p_idx == heading_page_idx and t_obj.bbox[3] < heading_top:
                    continue 

                rows = t_obj.extract()
                if not rows: continue
                
                # Check rows geometry if on the starting page to filter out header/above-text rows
                # This is crucial for Giant Tables
                if p_idx == heading_page_idx:
                     # Filter rows that are physically above the heading
                     # We can't use t_obj.rows directly with extracted text easily without mapping
                     # BUT we can check if the *first* extracted row looks like data we want?
                     # A safer way: iterate t_obj.rows and only keep those with bbox[1] > heading_top
                     
                     valid_indices = []
                     for r_idx, row_obj in enumerate(t_obj.rows):
                         if row_obj.bbox[1] > (heading_top + 2):
                             valid_indices.append(r_idx)
                     
                     if not valid_indices:
                         continue
                         
                     # Now subset the extracted rows
                     # rows is a list of lists.
                     rows = [rows[i] for i in valid_indices]

                cleaned_rows = []
                for r in rows:
                     if r: cleaned_rows.append([clean_cell(c) for c in r])
                if not cleaned_rows: continue

                maxcols = max(len(r) for r in cleaned_rows)
                cleaned_rows = [r + [""] * (maxcols - len(r)) for r in cleaned_rows]

                for r in cleaned_rows:
                    row_text = " ".join(r).upper()
                    row_text_clean = row_text.replace(" ", "")
                    first_col = r[0].upper().strip()
                    first_col_clean = first_col.replace(" ", "")
                    
                    # 1. Detect start of ISGS section
                    if "STATION" in row_text and "CONSTITUENTS" in row_text:
                        continue 

                    if "ISGS" == first_col or ("ISGS" in row_text and "TOTAL" not in row_text):
                         current_section = "central_sector"
                         start_collecting = True
                         continue

                    # Improved JV Detection
                    if ("JOINT" in row_text and "VENTURE" in row_text and "TOTAL" not in row_text) or "JOINTVENTURE" in row_text.replace(" ", ""):
                        current_section = "joint_venture"
                        start_collecting = True
                        print("DEBUG: Switch to Joint Venture section")
                        continue

                    # STOP CONDITIONS / SWITCH
                    if "TOTAL" in row_text and "ISGS" in row_text:
                        # Append total row
                        if "central_sector" in tables_dict: tables_dict["central_sector"].append((p_idx+1, 0, r))
                        # Switch to JV implicitly if not already
                        current_section = "joint_venture" 
                        continue 

                    if "TOTAL" in row_text and ("JOINT" in row_text and "VENTURE" in row_text):
                         if "joint_venture" in tables_dict: tables_dict["joint_venture"].append((p_idx+1, 0, r))
                         # Strict stop here? Yes, usually JV is last for 3(B)
                         return tables_dict

                    # STRICT STOP for Renewable/State Sector/IPP
                    # Matches "4. State Sector", "IPP", "Renewable", "Solar"
                    if (first_col_clean.startswith("4(") or 
                        ("STATE" in first_col and "SECTOR" in row_text) or 
                        "RENEWABLE" in row_text or
                        "SOLAR" in row_text or
                        "WIND" in row_text or
                        "NBUN" in row_text or 
                        "BUN" in row_text or
                        "IPP" in row_text or
                        "INTER-REGIONAL" in row_text or 
                        "VOLTAGEPROFILE" in row_text_clean):
                        return tables_dict

                    # COLLECT DATA
                    if start_collecting and current_section:
                        if "INST." in row_text and "CAPACITY" in row_text: continue
                        if "MW" in row_text and "PEAK" in row_text: continue
                        
                        tables_dict[current_section].append((p_idx + 1, 0, r))
                        
                    # FALLBACK Central Sector
                    # RESTRICT fallback: Only if we see "NTPC" or "NEYVELI" AND we haven't hit STOP conditions
                    # And ensure we are not in Renewable section (checked above)
                    if not start_collecting:
                        if ("KUDGI" in first_col or "NEYVELI" in first_col or "NTPC" in first_col) and "SOLAR" not in row_text:
                            current_section = "central_sector"
                            start_collecting = True
                            tables_dict[current_section].append((p_idx + 1, 0, r))

    return tables_dict

# ---------------- Combined Command ----------------
class Command(BaseCommand):
    help = "Download SRLDC PSP PDF, extract tables 2(A), 2(C) (tabula) and 3(B) (pdfplumber), save JSON & DB"

    def add_arguments(self, parser):
        parser.add_argument('--date', type=str, help='Date for which to run the report, format: YYYY-MM-DD',
                            required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # logger setup
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'srldc_full.log')
        self.logger = logging.getLogger('srldc_full_logger')
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # helpers
        self.tabula_extractor = TabulaExtractor(self.write, self.logger)

    def write(self, message, level='info'):
        try:
            self.stdout.write(message)
        except Exception:
            print(message)
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)

    # Reuse the download_latest_srldc_pdf from your tabula script (keeps same naming)
    def download_latest_srldc_pdf(self, base_url="https://www.srldc.in/var/ftp/reports/psp/",
                                  base_download_dir="downloads", given_date=None):
        project_name = "SRLDC"
        base_download_dir = os.path.join(base_download_dir, project_name)
        os.makedirs(base_download_dir, exist_ok=True)

        pdf_path = None
        report_date = None
        report_dir = None

        IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

        if given_date:
            today = datetime.datetime.strptime(given_date, "%Y-%m-%d").replace(tzinfo=IST) - datetime.timedelta(days=1)
        else:
            today = datetime.datetime.now(datetime.timezone.utc).astimezone(IST) - datetime.timedelta(days=1)

        current_date = today
        year = current_date.year
        month_abbr = current_date.strftime('%b').capitalize()
        day = current_date.day

        directory_path_on_server = f"{year}/{month_abbr}{str(year)[-2:]}/"
        file_name_on_server = f"{day:02d}-{current_date.month:02d}-{year}-psp.pdf"

        full_url = f"{base_url}{directory_path_on_server}{file_name_on_server}"

        if given_date:
            now_str = f"{given_date}_{datetime.datetime.now().strftime('%H-%M-%S')}"
        else:
            now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        report_dir = os.path.join(base_download_dir, f"report_{now_str}")
        os.makedirs(report_dir, exist_ok=True)
        self.write(f"📁 Checking/Created report directory: {report_dir}")

        folder_date_part = now_str.split('_')[0] if now_str else None
        try:
            if folder_date_part:
                file_date_str = datetime.datetime.strptime(folder_date_part, '%Y-%m-%d').strftime('%d%m%Y')
            else:
                file_date_str = current_date.strftime('%d%m%Y')
        except Exception:
            file_date_str = current_date.strftime('%d%m%Y')

        local_pdf_filename = f"srldc_{file_date_str}.pdf"
        local_file_path = os.path.join(report_dir, local_pdf_filename)

        if os.path.exists(local_file_path):
            self.write(self.style.NOTICE(
                f"📄 PDF already exists locally for {current_date.strftime('%d-%m-%Y')} at {local_file_path}. Skipping download."))
            pdf_path = local_file_path
            report_date = current_date.date()
            return pdf_path, report_date, report_dir

        self.write(f"🌐 Attempting to download from: {full_url}")
        try:
            session = requests.Session()
            session.mount('https://', LegacySSLAdapter())
            response = session.get(full_url, stream=True, timeout=60)
            response.raise_for_status()
            with open(local_file_path, 'wb') as pdf_file:
                for chunk in response.iter_content(chunk_size=8192):
                    pdf_file.write(chunk)
            self.write(self.style.SUCCESS(f"✅ Successfully downloaded: {local_pdf_filename} to {report_dir}"))
            pdf_path = local_file_path
            report_date = current_date.date()
            return pdf_path, report_date, report_dir
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.write(
                    self.style.WARNING(f"⚠️ File not found for {current_date.strftime('%d-%m-%Y')} at {full_url}."))
            else:
                self.write(self.style.ERROR(
                    f"❌ HTTP Error {e.response.status_code} while downloading {file_name_on_server}: {e}"))
        except Exception as e:
            self.write(self.style.ERROR(f"❌ An unexpected error occurred during download: {e}"))

        return None, None, None

    # small wrappers mirroring Django style utilities used earlier
    def style(self, kind):
        class S:
            def __init__(self, k): self.k = k

            def SUCCESS(self, m): return m

            def WARNING(self, m): return m

            def ERROR(self, m): return m

            def NOTICE(self, m): return m

        return S(kind)

    # expose convenience methods used in TabulaExtractor
    def style_warning(self, msg):
        return msg

    def handle(self, *args, **options):
        # download pdf (same logic as your tabula script)
        pdf_path, report_date, report_output_dir = self.download_latest_srldc_pdf(given_date=options.get('date'))

        if pdf_path is None:
            self.write(self.style.ERROR("No PDF report was successfully downloaded or found locally. Exiting."),
                       level='error')
            return

        # 1. Initialize result container
        combined_json_data = {}

        try:
             # We rely mainly on pdfplumber now, so skipping direct Tabula read_pdf for 2A/2C to avoid overhead/confusion.
             # If we needed generic table extraction we could use it, but existing logic ignores it.
            
             # ------------------ NEW Table 2(A) Logic (pdfplumber) ------------------
                self.write("🔍 Extracting Table 2(A) using pdfplumber heading anchor...", level='info')
                df_2A = extract_table_2A_using_heading(pdf_path)

                if df_2A is None:
                    self.write("❌ Table 2(A) NOT FOUND via heading anchor.", level='warning')
                else:
                    self.write("✅ Table 2(A) FOUND via heading anchor.", level='success')

                    try:
                        # ---- FIX HEADER (Handle 2-row header) ----
                        # Row 0 and Row 1 are headers. We consolidate them to identify columns correctly.
                        # This prevents "State's Control Area..." from being confused with "State".
                        h0_vals = df_2A.iloc[0].fillna("").astype(str).str.upper().str.replace("\n", " ").str.strip().tolist()
                        h1_vals = df_2A.iloc[1].fillna("").astype(str).str.upper().str.replace("\n", " ").str.strip().tolist()
                        
                        final_cols = []
                        for h0, h1 in zip(h0_vals, h1_vals):
                            # Identify columns based on header content in Row 1 (Generation cols) or Row 0 (others)
                            if "THERMAL" in h1: final_cols.append("THERMAL")
                            elif "HYDRO" in h1: final_cols.append("HYDRO")
                            elif "GAS" in h1 or "DIESEL" in h1 or "NAPTHA" in h1: final_cols.append("GAS")
                            elif "WIND" in h1: final_cols.append("WIND")
                            elif "SOLAR" in h1: final_cols.append("SOLAR")
                            elif "OTHERS" in h1: final_cols.append("OTHERS")
                            
                            # State column - standard "State" header, exclude the super-header "State's Control..."
                            elif "STATE" in h0 and "CONTROL" not in h0 and "GENERATION" not in h0: final_cols.append("STATE")
                            
                            # Other columns mostly in Row 0
                            elif "NET SCH" in h0: final_cols.append("NET SCH")
                            elif "DRAWAL" in h0: final_cols.append("DRAWAL")
                            elif "UI" in h0: final_cols.append("UI")
                            elif "AVAILABILITY" in h0: final_cols.append("AVAILABILITY")
                            elif "DEMAND MET" in h0: final_cols.append("DEMAND MET")
                            elif "SHORTAGE" in h0: final_cols.append("SHORTAGE")
                            else:
                                final_cols.append(h0 if h0 else h1)

                        df_2A.columns = final_cols
                        # Drop the two header rows
                        df_2A = df_2A.iloc[2:].reset_index(drop=True)

                        # ---- COLUMN MAPPING ----
                        column_mapping = {
                            "STATE": "state",
                            "THERMAL": "thermal",
                            "HYDRO": "hydro",
                            "GAS": "gas_naptha_diesel",
                            "SOLAR": "solar",
                            "WIND": "wind",
                            "OTHERS": "others",
                            "NET SCH": "net_sch",
                            "DRAWAL": "drawal",
                            "UI": "ui",
                            "AVAILABILITY": "availability",
                            "DEMAND MET": "demand_met",
                            "SHORTAGE": "shortage",
                        }

                        # Apply mapping
                        for col in list(df_2A.columns):
                            for key in column_mapping:
                                if key in col:
                                    df_2A.rename(columns={col: column_mapping[key]}, inplace=True)

                        # Filter valid columns
                        valid_cols = [c for c in column_mapping.values() if c in df_2A.columns]
                        df_2A = df_2A[valid_cols]
                        
                        if 'state' in df_2A.columns:
                            # Normalize state column to handle newlines
                            df_2A['state'] = df_2A['state'].astype(str).str.replace("\n", " ").str.strip()

                            # Filter valid states to remove footer text
                            valid_states = ["ANDHRA PRADESH", "KARNATAKA", "KERALA", "TAMIL NADU", "TAMILNADU", "TELANGANA", "PUDUCHERRY", "PONDICHERRY", "REGION"]
                            df_2A = df_2A[df_2A['state'].str.upper().isin(valid_states)]
                            
                            # Truncate at 'Region' to avoid picking up subsequent tables (like Wind/Solar generation)
                            # Identify the index of 'Region'
                            temp_df_2a = df_2A.copy().reset_index(drop=True)
                            temp_df_2a['state_upper'] = temp_df_2a['state'].astype(str).str.strip().str.upper()
                            
                            region_indices = temp_df_2a.index[temp_df_2a['state_upper'] == 'REGION'].tolist()
                            if region_indices:
                                cutoff_idx = region_indices[0]
                                df_2A = temp_df_2a.loc[:cutoff_idx].drop(columns=['state_upper'])
                            else:
                                df_2A = temp_df_2a.drop(columns=['state_upper'])

                            df_2A = df_2A.dropna(subset=["state"])
                            
                            # Convert state to Title Case for consistency
                            df_2A['state'] = df_2A['state'].astype(str).str.title()
                            
                            # Add to combined JSON
                            combined_json_data['srldc_table_2A'] = df_2A.to_dict(orient="records")
                            self.write(f"✅ Table 2(A) processed. {len(df_2A)} rows.", level='info')

                            # ================= SAVE TO DB =================
                            # ================= SAVE TO DB ================= 
                            # Updated column mapping to include net_sch, demand_met as requested
                            for _, row in df_2A.iterrows():
                                try:
                                    Srldc2AData.objects.update_or_create(
                                        report_date=report_date,
                                        state=row["state"],
                                        defaults={
                                            "thermal": self.tabula_extractor._safe_float(row.get("thermal")),
                                            "hydro": self.tabula_extractor._safe_float(row.get("hydro")),
                                            "gas_naptha_diesel": self.tabula_extractor._safe_float(row.get("gas_naptha_diesel")),
                                            "solar": self.tabula_extractor._safe_float(row.get("solar")),
                                            "wind": self.tabula_extractor._safe_float(row.get("wind")),
                                            "others": self.tabula_extractor._safe_float(row.get("others")),
                                            "net_sch": self.tabula_extractor._safe_float(row.get("net_sch")),
                                            "drawal": self.tabula_extractor._safe_float(row.get("drawal")),
                                            "ui": self.tabula_extractor._safe_float(row.get("ui")),
                                            "availability": self.tabula_extractor._safe_float(row.get("availability")),
                                            "demand_met": self.tabula_extractor._safe_float(row.get("demand_met")),
                                            "shortage": self.tabula_extractor._safe_float(row.get("shortage")),
                                        }
                                    )
                                except Exception as e:
                                    self.write(f"❌ Error saving Table 2A row (State: {row.get('state')}): {e}", level='error')
                            
                            self.write(f"✅ Saved/Updated rows to Srldc2AData DB", level='success')
                        else:
                             self.write("⚠️ 'state' column missing in 2(A) dataframe after mapping.", level='warning')

                    except Exception as e:
                        self.write(f"❌ Error processing Table 2(A) data: {e}", level='error')
                        self.write(traceback.format_exc(), level='error')

                # ------------------ NEW Table 2(C) Logic (pdfplumber) ------------------
                self.write("🔍 Extracting Table 2(C) using pdfplumber heading anchor...", level='info')
                df_2C = extract_table_2C_using_heading(pdf_path)

                if df_2C is None:
                    self.write("❌ Table 2(C) NOT FOUND via heading anchor.", level='warning')
                else:
                    self.write("✅ Table 2(C) FOUND via heading anchor.", level='success')
                    self.write("--- RAW DataFrame for Table 2(C) ---", level='info')
                    # self.write(str(df_2C.head()), level='info')

                    try:
                        # 2(C) Column Mapping (Straightforward index mapping based on fixed PDF structure)
                        # Expected 13 columns as per user request/screenshot:
                        # 0: State
                        # 1: Max Demand Met
                        # 2: Time
                        # 3: Shortage
                        # 4: Req at Max Demand
                        # 5: Demand Met at Max Req
                        # 6: Time
                        # 7: Shortage
                        # 8: Max Req Day
                        # 9: Max ACE (ACE(MW))
                        # 10: Time
                        # 11: Min ACE (ACE(MW))
                        # 12: Time
                        
                        states_found = []
                        processed_2c = []
                        
                        # Iterate raw rows and clean them
                        for row in df_2C.itertuples(index=False):
                            try:
                                # Clean row to remove empty columns from pdfplumber artifacts
                                row_clean = [x for x in row if x is not None and str(x).strip() != ""]
                                
                                if not row_clean: continue
                                
                                # We need at least State + something
                                if len(row_clean) < 2: continue

                                state_raw = str(row_clean[0]).strip()
                                state_norm = state_raw.replace('\n', ' ').strip().title()
                                
                                # Fix for abbreviated states in Table 2(C)
                                state_map = {
                                    "AP": "Andhra Pradesh",
                                    "KAR": "Karnataka",
                                    "KER": "Kerala",
                                    "PONDY": "Pondicherry",
                                    "TN": "Tamilnadu",
                                    "TG": "Telangana",
                                    "REGION": "Region"
                                }
                                
                                # Check exact match or lookup
                                if state_norm.upper() in state_map:
                                    state_norm = state_map[state_norm.upper()]
                                
                                # Fallback regex check still useful if full names appear
                                if not re.match(r'^(Andhra|Karnataka|Kerala|Tamil|Telangana|Pondicherry|Region)', state_norm, re.IGNORECASE):
                                     continue
                                     
                                states_found.append(state_norm)

                                # Helper for safe access from cleaned list
                                def get_val(idx):
                                    return row_clean[idx] if idx < len(row_clean) else None
                                
                                rec = {
                                   "state": state_norm,
                                   "max_demand": parse_float_safe(get_val(1)),
                                   "time": str(get_val(2)) if get_val(2) else None,
                                   "shortage_max_demand": parse_float_safe(get_val(3)),
                                   "req_max_demand": parse_float_safe(get_val(4)),
                                   "demand_max_req": parse_float_safe(get_val(5)),
                                   "time_max_req": str(get_val(6)) if get_val(6) else None,
                                   "shortage_max_req": parse_float_safe(get_val(7)),
                                   "max_req_day": parse_float_safe(get_val(8)),
                                   "ace_max": parse_float_safe(get_val(9)),
                                   "time_ace_max": str(get_val(10)) if get_val(10) else None,
                                   "ace_min": parse_float_safe(get_val(11)),
                                   "time_ace_min": str(get_val(12)) if get_val(12) else None,
                                }
                                
                                processed_2c.append(rec)

                                # DB Save
                                try:
                                     Srldc2CData.objects.update_or_create(
                                         report_date=report_date,
                                         state=state_norm,
                                         defaults={k:v for k,v in rec.items() if k!='state'}
                                     )
                                except Exception as e:
                                     self.write(f"❌ Error saving Table 2C row to DB: {e}", level='error')

                            except Exception as e:
                                self.write(f"❌ Error row 2(C): {row} -> {e}", level='error')

                        combined_json_data['srldc_table_2C'] = processed_2c
                        self.write(f"✅ Table 2(C) processed. {len(processed_2c)} rows. States: {states_found}", level='info')

                    except Exception as e:
                        self.write(f"❌ Error processing Table 2(C): {e}", level='error')
                        self.write(traceback.format_exc(), level='error')

                # NOTE: removed separate combined 2A/2C JSON write here — will save single combined JSON after 3B extraction

        except Exception as e:
            self.write(f"❌ Tabula extraction failed: {e}", level='error')
            self.write(traceback.format_exc(), level='error')

        # ------------------ NEW Table 3(B) Logic (pdfplumber) ------------------
        try:
            self.write("🔍 Extracting Table 3(B) using pdfplumber heading anchor...", level='info')
            
            report_info = extract_report_dates(pdf_path)
            # 🔎 DEBUG: compare PDF internal date vs forced date
            self.write(
                f"PDF internal date: {report_info.get('report_date')} | "
                f"Forced report date: {report_date}",
                level="warning"
            )

            # 🔒 FORCE correct report date everywhere
            report_info["report_date"] = report_date.isoformat()

            # 🔒 FORCE correct report date (ignore PDF internal date)
            report_info["report_date"] = report_date.isoformat()

            self.write(f"Report DATE (3B extraction): {report_info.get('report_date')}", level='info')
            self.write(f"Reporting DATETIME (3B extraction): {report_info.get('reporting_datetime')}", level='info')

            # Using the new layout-based extractor
            tables_3b = extract_table_3B_using_heading(pdf_path)
            
            central_cnt = len(tables_3b["central_sector"])
            jv_cnt = len(tables_3b["joint_venture"])
            
            if central_cnt == 0 and jv_cnt == 0:
                self.write("❌ Table 3(B) NOT FOUND via heading anchor.", level='warning')
            else:
                self.write(f"✅ Table 3(B) FOUND via heading anchor. Central: {central_cnt}, JV: {jv_cnt}", level='success')

            # Normalize (parsing numbers)
            central_3b = normalize_rows_for_table_3b(tables_3b.get("central_sector", []), report_info)
            jv_3b = normalize_rows_for_table_3b(tables_3b.get("joint_venture", []), report_info)
            combined_3b = central_3b + jv_3b

            # JSON snapshot
            snapshot_3b = {
                "report_date": report_info.get("report_date"),
                "reporting_datetime": report_info.get("reporting_datetime"),
                "central_sector": central_3b,
                "joint_venture": jv_3b
            }

            # ------------------ BUILD SINGLE COMBINED JSON ------------------
            final_payload = {
                "srldc_table_2A": combined_json_data.get("srldc_table_2A", []),
                "srldc_table_2C": combined_json_data.get("srldc_table_2C", []),
                "srldc_table_3B": {
                    "central_sector": snapshot_3b.get("central_sector", []),
                    "joint_venture": snapshot_3b.get("joint_venture", [])
                }
            }

            combined_master_path = os.path.join(report_output_dir, f"srldc_combined_{report_date}.json")
            try:
                with open(combined_master_path, 'w', encoding='utf-8') as mf:
                    json.dump(final_payload, mf, indent=4, ensure_ascii=False, default=str)
                self.write(f"✅ Final combined JSON saved: {combined_master_path}", level='success')
            except Exception as e:
                self.write(f"❌ Failed to write final combined JSON: {e}", level='error')

            # Save 3B to DB
            saved = 0
            with transaction.atomic():
                for rec in combined_3b:
                    # parse report_date for DB
                    if isinstance(report_info.get("report_date"), str):
                        report_date_parsed = _try_parse_date_token(report_info.get("report_date"))
                    else:
                        report_date_parsed = report_info.get("report_date")

                    # parse reporting datetime
                    reporting_dt = None
                    if report_info.get("reporting_datetime"):
                        try:
                            naive = _dt.strptime(report_info.get("reporting_datetime"), "%Y-%m-%d %H:%M")
                            if settings.USE_TZ:
                                reporting_dt = timezone.make_aware(naive)
                            else:
                                reporting_dt = naive
                        except Exception:
                            reporting_dt = None

                    # 🔒 FIX: reporting_datetime must never be NULL
                    # Frontend/API treats NULL as "no data"
                    if reporting_dt is None and report_date_parsed:
                        naive_dt = _dt.combine(report_date_parsed, _dt.min.time())
                        if settings.USE_TZ:
                            reporting_dt = timezone.make_aware(naive_dt)
                        else:
                            reporting_dt = naive_dt

                    def to_dec(v):
                        if v is None: return None
                        try: return Decimal(str(v))
                        except: return None
                    try:
                        obj, created = SRLDC3BData.objects.update_or_create(
                            station=rec.get("station"),
                            report_date=report_date_parsed,
                            defaults={
                                "reporting_datetime": reporting_dt,
                                "installed_capacity_mw": rec.get("installed_capacity_mw"),
                                "peak_1900_mw": rec.get("peak_1900_mw"),
                                "offpeak_0300_mw": rec.get("offpeak_0300_mw"),
                                "day_peak_mw": rec.get("day_peak_mw"),
                                "day_peak_hrs": rec.get("day_peak_hrs"),
                                "min_generation_mw": to_dec(rec.get("min_generation_mw")),
                                "min_generation_hrs": rec.get("min_generation_hrs"),
                                "gross_energy_mu": to_dec(rec.get("gross_energy_mu")),
                                "net_energy_mu": to_dec(rec.get("net_energy_mu")),
                                "avg_mw": to_dec(rec.get("avg_mw")),
                                "row_type": rec.get("row_type"),
                                "source_page": rec.get("source_page"),
                                "source_table_index": rec.get("source_table_index"),
                            }
                        )
                        saved += 1
                    except Exception as e:
                        self.write(f"❌ DB save error for station '{rec.get('station')}' : {e}", level='error')
                        self.write(traceback.format_exc(), level='error')

            self.write(f"Saved {saved} rows to SRLDC3BData for {report_date}", level='success')

        except Exception as e:
            self.write(f"❌ 3B extraction failed: {e}", level='error')
            self.write(traceback.format_exc(), level='error')

        self.write(f"Finished processing. Files saved in: {report_output_dir}", level='success')
