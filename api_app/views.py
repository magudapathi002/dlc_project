# views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from processor.models import Srldc2AData, Srldc2CData, Nrldc2CData, Nrldc2AData, Wrldc2AData, Wrldc2CData, PosocoTableA, \
    PosocoTableG, SRLDC3BData
from .serializers import SrldcASerializer, SrldcCSerializer, NrldcASerializer, NrldcCSerializer, WrldcASerializer, WrldcCSerializer, PosocoGSerializer, PosocoASerializer


from datetime import datetime, timedelta
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status



@api_view(['GET'])
@permission_classes([AllowAny])
def srldc_view(request):
    date_param = request.GET.get("date")
    month = request.GET.get("month")
    year = request.GET.get("year")

    # =========================================================
    # 🟢 MODE 1: MONTHLY MODE (month click)
    # =========================================================
    if month and year:
        try:
            month = int(month)
            year = int(year)
        except ValueError:
            return Response(
                {"error": "Invalid month/year"},
                status=status.HTTP_400_BAD_REQUEST
            )

        a_tab = Srldc2AData.objects.filter(
            report_date__year=year,
            report_date__month=month
        )
        c_tab = Srldc2CData.objects.filter(
            report_date__year=year,
            report_date__month=month
        )
        b_tab = SRLDC3BData.objects.filter(
            report_date__year=year,
            report_date__month=month
        )
        print(a_tab.query,"a_tab")
        print(c_tab.query,"c_tab")
        print(b_tab.query,"b_tab")

        print("📦 MONTHLY RAW:", a_tab)

        return Response(
            {
                "mode": "monthly",
                "month": f"{year}-{month:02d}",
                "record_count": {
                    "table_a": a_tab.count(),
                    "table_c": c_tab.count(),
                    "table_b": b_tab.count(),
                },
                "table_a": SrldcASerializer(a_tab, many=True).data,
                "table_c": SrldcCSerializer(c_tab, many=True).data,
                "table_b": list(b_tab.values()),
            },
            status=status.HTTP_200_OK
        )

    # =========================================================
    # 🟢 MODE 2: DAILY MODE (old behaviour – date click)
    # =========================================================
    if not date_param:
        requested_date = datetime.today().date()
    else:
        try:
            requested_date = datetime.strptime(date_param, "%Y-%m-%d").date()
        except ValueError:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST
            )

    report_date = requested_date

    a_tab = Srldc2AData.objects.filter(report_date=report_date)
    c_tab = Srldc2CData.objects.filter(report_date=report_date)
    b_tab = SRLDC3BData.objects.filter(report_date=report_date)

    # ---------- fallback only for DAILY ----------
    if not a_tab.exists() and not c_tab.exists() and not b_tab.exists():
        report_date = requested_date - timedelta(days=1)

        a_tab = Srldc2AData.objects.filter(report_date=report_date)
        c_tab = Srldc2CData.objects.filter(report_date=report_date)
        b_tab = SRLDC3BData.objects.filter(report_date=report_date)

        if not a_tab.exists() and not c_tab.exists() and not b_tab.exists():
            return Response(
                {"error": "No data available"},
                status=status.HTTP_404_NOT_FOUND
            )

    return Response(
        {
            "mode": "daily",
            "requested_date": str(requested_date),
            "actual_report_date": str(report_date),
            "record_count": {
                "table_a": a_tab.count(),
                "table_c": c_tab.count(),
                "table_b": b_tab.count(),
            },
            "table_a": SrldcASerializer(a_tab, many=True).data,
            "table_c": SrldcCSerializer(c_tab, many=True).data,
            "table_b": list(b_tab.values()),
        },
        status=status.HTTP_200_OK
    )



@api_view(['GET'])
@permission_classes([AllowAny])
def nrldc_view(request):
    a_tab = Nrldc2AData.objects.all()
    c_tab = Nrldc2CData.objects.all()
    return Response({
        "table_a": NrldcASerializer(a_tab, many=True).data,
        "table_c": NrldcCSerializer(c_tab, many=True).data
    }, status=status.HTTP_200_OK)

@api_view(['GET'])
@permission_classes([AllowAny])
def wrldc_view(request):
    a_tab = Wrldc2CData.objects.all()
    c_tab = Wrldc2AData.objects.all()
    return Response({
        "table_a": WrldcASerializer(a_tab, many=True).data,
        "table_c": WrldcCSerializer(c_tab, many=True).data
    }, status=status.HTTP_200_OK)


@api_view(['GET'])
@permission_classes([AllowAny])
def posoco_view(request):
    a_tab = PosocoTableA.objects.all()
    c_tab = PosocoTableG.objects.all()
    return Response({
        "table_a": PosocoASerializer(a_tab, many=True).data,
        "table_c": PosocoGSerializer(c_tab, many=True).data
    }, status=status.HTTP_200_OK)