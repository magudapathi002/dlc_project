from datetime import date
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import requests
from datetime import date, timedelta, datetime
from datetime import timedelta, datetime
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
import calendar
from datetime import date, datetime




SRLDC_API_BASE = "http://127.0.0.1:8000/api/srldc/"

def fetch_srldc_data(params: dict):
    """
    Helper to fetch SRLDC data from the local API.
    Returns parsed JSON on success or None on failure.
    """
    try:
        resp = requests.get(SRLDC_API_BASE, params=params, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        # You can log the exception here if you have logging configured
        return None


# add these imports at the top of your views.py (if not already)
@login_required
def psp(request):
    """
    PSP page:
    - Accepts GET param 'date' (preferred) or 'report_date' (fallback).
    - If no date provided, default to yesterday.
    - Does not block future dates (treats any valid YYYY-MM-DD as valid).
    """
    selected_state = request.session.get("selected_state", "").strip()

    # default fallback -> yesterday (keeps previous behavior when no date provided)
    yesterday_iso = (date.today() - timedelta(days=1)).isoformat()

    # prefer 'date' param (matches template/JS). fall back to 'report_date' if present.
    date_param = (request.GET.get("date") or request.GET.get("report_date") or "").strip()

    if date_param:
        # validate format; if invalid, fallback to yesterday
        try:
            parsed_date = datetime.strptime(date_param, "%Y-%m-%d").date()
            selected_date = parsed_date.isoformat()   # accept any valid date (past/present/future)
        except ValueError:
            selected_date = yesterday_iso
    else:
        selected_date = yesterday_iso

    # debug log (will appear in runserver console)
    print("PSP selected_date =", selected_date, "selected_state =", selected_state)

    # Fetch SRLDC data for this state + date (same as before)
    srldc_params = {"state": selected_state or "", "date": selected_date}
    srldc_raw = fetch_srldc_data(srldc_params)

    # determine initial_processor (best-effort)
    initial_processor = None
    processors = None
    if srldc_raw:
        if isinstance(srldc_raw, dict):
            # try keys that commonly contain lists
            for key in ("processor", "processor_data", "table_a", "data", "results", "processors"):
                if key in srldc_raw and isinstance(srldc_raw[key], list):
                    processors = srldc_raw[key]
                    break
            # fallback: first list value in dict
            if processors is None:
                for k, v in srldc_raw.items():
                    if isinstance(v, list):
                        processors = v
                        break
        elif isinstance(srldc_raw, list):
            processors = srldc_raw

    if processors and isinstance(processors, list) and len(processors) > 0:
        def norm(s): return (s or "").strip().lower().replace(" ", "")
        want = (selected_state or "").strip().lower().replace(" ", "")
        # try find matching state (best-effort)
        found = next((p for p in processors if norm(p.get("state")) == want), None)
        if not found:
            found = next((p for p in processors if norm(p.get("state", "")).startswith(want)), None)
        if not found:
            found = next((p for p in processors if want and want in norm(p.get("state", ""))), None)
        if not found:
            # try matching by date/report_date too
            found = next((p for p in processors if str(p.get("report_date", p.get("reportdate", ""))) == selected_date), None)
        if not found:
            found = processors[0]
        initial_processor = found

    context = {
        "user": request.user,
        "title": "PSP",
        "selected_state": selected_state,
        "selected_date": selected_date,
        "today": date.today().isoformat(),
        "srldc_data": srldc_raw,
        "initial_processor": initial_processor,
    }
    return render(request, "dailyreports/psp.html", context)


@login_required
def error_report(request):
    selected_date = request.GET.get('date', '')   # keep how you were using it
    selected_state = request.GET.get('state', '')

    # hours list for the template (strings "01".."24")
    hours = [f"{i:02d}" for i in range(1, 25)]

    # Fetch SRLDC data (filter by state + date)
    srldc_params = {"state": selected_state, "date": selected_date}
    srldc_data = fetch_srldc_data(srldc_params)

    context = {
        'selected_date': selected_date,
        'selected_state': selected_state,
        'hours': hours,
        'srldc_data': srldc_data,
    }

    return render(request, 'dailyreports/error_report.html', context)




@login_required
def monthly_error_report(request):
    # -----------------------------
    # Month / Year handling
    # -----------------------------
    today = date.today()
    month = request.GET.get("month") or f"{today.month:02d}"
    year = request.GET.get("year") or str(today.year)

    try:
        m = int(month)
        y = int(year)
    except ValueError:
        m = today.month
        y = today.year

    # -----------------------------
    # Fetch SRLDC payload
    # -----------------------------
    srldc_params = {
        "month": f"{m:02d}",
        "year": str(y),
        "type": "monthly"
    }
    raw = fetch_srldc_data(srldc_params) or {}

    # -----------------------------
    # Normalize payload → records
    # -----------------------------
    records = []

    if isinstance(raw, dict):
        if isinstance(raw.get("table_a"), (list, tuple)):
            records = list(raw["table_a"])
        else:
            # fallback: first list-like value
            for v in raw.values():
                if isinstance(v, (list, tuple)):
                    records = list(v)
                    break

    elif isinstance(raw, (list, tuple)):
        records = list(raw)

    # -----------------------------
    # Helpers
    # -----------------------------
    def normalize_state(val):
        if not val:
            return ""
        return str(val).strip().lower().replace(" ", "")

    # -----------------------------
    # Extract Tamil Nadu WIND by date
    # -----------------------------
    tn_wind_by_date = {}

    for rec in records:
        if not isinstance(rec, dict):
            continue

        state = normalize_state(rec.get("state"))
        if state not in ("tamilnadu", "tn"):
            continue

        report_date = rec.get("report_date")
        if not report_date:
            continue

        iso_date = str(report_date)[:10]

        wind_raw = rec.get("wind")
        try:
            wind_val = float(wind_raw)
        except (TypeError, ValueError):
            wind_val = None

        if wind_val is not None:
            tn_wind_by_date[iso_date] = wind_val

    # -----------------------------
    # Build month rows
    # -----------------------------
    _, ndays = calendar.monthrange(y, m)

    rows = []
    total_actual = 0.0
    actual_counted = 0

    for d in range(1, ndays + 1):
        iso = f"{y:04d}-{m:02d}-{d:02d}"
        display = f"{d:02d}-{date(y, m, d).strftime('%b')}-{y}"

        actual_val = tn_wind_by_date.get(iso)

        if actual_val is not None:
            total_actual += actual_val
            actual_counted += 1

        rows.append({
            "date_iso": iso,
            "date_display": display,
            "actual": actual_val
        })

    total_display = round(total_actual, 2) if actual_counted else None

    # -----------------------------
    # Dropdown helpers (NO JS)
    # -----------------------------
    years = list(range(today.year - 5, today.year + 6))
    months = [
        {"value": "01", "label": "January"},
        {"value": "02", "label": "February"},
        {"value": "03", "label": "March"},
        {"value": "04", "label": "April"},
        {"value": "05", "label": "May"},
        {"value": "06", "label": "June"},
        {"value": "07", "label": "July"},
        {"value": "08", "label": "August"},
        {"value": "09", "label": "September"},
        {"value": "10", "label": "October"},
        {"value": "11", "label": "November"},
        {"value": "12", "label": "December"},
    ]

    # -----------------------------
    # Debug info (optional)
    # -----------------------------
    debug_info = {
        "records_count": len(records),
        "tn_dates_found": sorted(tn_wind_by_date.keys()),
        "tn_sample": {
            k: tn_wind_by_date[k]
            for k in sorted(tn_wind_by_date.keys())[:8]
        }
    }

    # -----------------------------
    # Context
    # -----------------------------
    context = {
        "rows": rows,
        "total_actual": total_display,
        "selected_month": f"{m:02d}",
        "selected_year": str(y),
        "months": months,
        "years": years,
        "debug_info": debug_info,
    }

    return render(
        request,
        "dailyreports/monthly_error_report.html",
        context
    )


# --- Add these new views --- #
@login_required
def daily_comparison(request):
    """
    Renders the Daily Comparison - Chart page.
    Accessible via {% url 'daily_comparison' %}
    """
    selected_state = request.session.get("selected_state")
    # optional GET params you might use for chart filters
    date_value = request.GET.get("date")
    month = request.GET.get("month")
    year = request.GET.get("year")

    # Fetch SRLDC data for the requested filter set
    srldc_params = {"state": selected_state or "", "date": date_value or "", "month": month or "", "year": year or "", "type": "daily_comparison"}
    srldc_data = fetch_srldc_data(srldc_params)

    context = {
        "user": request.user,
        "title": "Daily Comparison - Chart",
        "selected_state": selected_state,
        "date": date_value,
        "month": month,
        "year": year,
        "preview_image": "/mnt/data/979ee8e8-59f4-47a7-add1-68f484a32caf.png",
        "srldc_data": srldc_data,
    }
    return render(request, "dailyreports/daily_comparison.html", context)


@login_required
def daily_windy_power(request):
    """
    Renders the Daily Wind Report Generation page.
    Accessible via {% url 'daily_windy_power' %}
    """
    selected_state = request.session.get("selected_state")
    # optional GET params for the report generation
    date_value = request.GET.get("date")

    srldc_params = {"state": selected_state or "", "date": date_value or "", "type": "windy"}
    srldc_data = fetch_srldc_data(srldc_params)

    params = {
        "user": request.user,
        "title": "Daily Wind Report Generation",
        "selected_state": selected_state,
        "date": date_value,
        "srldc_data": srldc_data,
    }
    return render(request, "dailyreports/daily_windy_power.html", params)


@login_required
def run_daily_dsm(request):
    """
    Renders a page to trigger/run the Daily DSM process.
    Accessible via {% url 'run_daily_dsm' %}
    If this view should actually trigger a backend job, add that logic here.
    """
    selected_state = request.session.get("selected_state")
    # Example: read optional job parameters from GET
    run_for_date = request.GET.get("run_date")

    # fetch SRLDC info related to DSM or the date/state in question
    srldc_params = {"state": selected_state or "", "date": run_for_date or "", "type": "dsm"}
    srldc_data = fetch_srldc_data(srldc_params)

    context = {
        "user": request.user,
        "title": "RUN Daily DSM",
        "selected_state": selected_state,
        "run_for_date": run_for_date,
        "preview_image": "/mnt/data/979ee8e8-59f4-47a7-add1-68f484a32caf.png",
        "srldc_data": srldc_data,
    }
    return render(request, "dailyreports/run_daily_dsm.html", context)


@login_required
def accuracy_report(request):
    """
    Renders the Accuracy Report & Remarks page.
    Accessible via {% url 'Accuracy_report' %}
    Note: view name matches your template/URL name you provided.
    """
    selected_state = request.session.get("selected_state")
    # optional filters
    month = request.GET.get("month")
    year = request.GET.get("year")

    srldc_params = {"state": selected_state or "", "month": month or "", "year": year or "", "type": "accuracy"}
    srldc_data = fetch_srldc_data(srldc_params)

    context = {
        "user": request.user,
        "title": "Accuracy Report & Remarks",
        "selected_state": selected_state,
        "month": month,
        "year": year,
        "srldc_data": srldc_data,
    }
    return render(request, "dailyreports/accuracy_report.html", context)
