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
import pandas as pd
from django.http import HttpResponse





SRLDC_API_BASE = "http://127.0.0.1:1236/api/srldc/"

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
    # Allow GET param to override session, otherwise fallback to session
    selected_state = request.GET.get("state") or request.session.get("selected_state", "")
    selected_state = selected_state.strip()

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
    import calendar
    import pandas as pd
    from datetime import date
    from django.http import HttpResponse
    from django.shortcuts import render

    today = date.today()
    month = request.GET.get("month") or f"{today.month:02d}"
    year = request.GET.get("year") or str(today.year)

    months = [
        {"value": f"{i:02d}", "label": calendar.month_name[i]}
        for i in range(1, 13)
    ]
    years = [str(y) for y in range(2015, today.year + 1)]

    # ---------------- Fetch data ----------------
    srldc_params = {"month": month, "year": year, "type": "monthly"}
    raw = fetch_srldc_data(srldc_params) or {}

    records = raw.get("table_a", []) if isinstance(raw, dict) else raw

    def normalize(s):
        return str(s).lower().replace(" ", "") if s else ""

    tn_wind_by_date = {}

    for r in records:
        if normalize(r.get("state")) in ("tamilnadu", "tn"):
            d = str(r.get("report_date"))[:10]
            try:
                tn_wind_by_date[d] = float(r.get("wind"))
            except:
                pass

    m, y = int(month), int(year)
    _, ndays = calendar.monthrange(y, m)

    rows, total = [], 0.0
    for d in range(1, ndays + 1):
        iso = f"{y}-{m:02d}-{d:02d}"
        val = tn_wind_by_date.get(iso)
        if val:
            total += val
        rows.append({
            "date_display": f"{d:02d}-{calendar.month_abbr[m]}-{y}",
            "actual": val
        })

    total_display = round(total, 2)

    # ================== ✅ EXCEL EXPORT (POST ONLY) ==================
    if request.method == "POST" and request.POST.get("export") == "excel":
        excel_rows = [{
            "Date": r["date_display"],
            "Forecast (MU)": "",
            "Actual (MU)": r["actual"],
            "Abs. Dev": "",
            "Abs. Error %": ""
        } for r in rows]

        excel_rows.append({
            "Date": "Total",
            "Forecast (MU)": "",
            "Actual (MU)": total_display,
            "Abs. Dev": "",
            "Abs. Error %": ""
        })

        df = pd.DataFrame(excel_rows)

        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = f'attachment; filename="Monthly_Report_{month}_{year}.xlsx"'
        df.to_excel(response, index=False)
        return response
    # ================================================================

    context = {
        "months": months,
        "years": years,
        "rows": rows,
        "total_actual": total_display,
        "selected_month": month,
        "selected_year": year,
    }

    return render(request, "dailyreports/monthly_error_report.html", context)





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
