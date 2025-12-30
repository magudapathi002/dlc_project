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
    date = request.GET.get("date")
    if not date:
        date = datetime.today().strftime("%Y-%m-%d")
    # if not date:
    #     return Response(
    #         {"error": "date parameter is required (YYYY-MM-DD)"},
    #         status=status.HTTP_400_BAD_REQUEST
    #     )

    # ---------------- DATE PARSING ----------------
    try:
        requested_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return Response(
            {"error": "Invalid date format. Use YYYY-MM-DD"},
            status=status.HTTP_400_BAD_REQUEST
        )

    # ---------------- FETCH DATA (NO STATE FILTER) ----------------
    report_date = requested_date

    a_tab = Srldc2AData.objects.filter(report_date=report_date)
    c_tab = Srldc2CData.objects.filter(report_date=report_date)

    # fallback to previous day ONLY if nothing exists
    if not a_tab.exists() and not c_tab.exists():
        report_date = requested_date - timedelta(days=1)
        a_tab = Srldc2AData.objects.filter(report_date=report_date)
        c_tab = Srldc2CData.objects.filter(report_date=report_date)

    # ---------------- TABLE 3(B) ----------------
    b_tab = SRLDC3BData.objects.filter(report_date=report_date)

    return Response(
        {
            "requested_date": str(requested_date),
            "actual_report_date": str(report_date),
            "table_a": SrldcASerializer(a_tab, many=True).data,  # ALL STATES
            "table_c": SrldcCSerializer(c_tab, many=True).data,  # ALL STATES
            "table_b": list(b_tab.values()),                     # ALL STATIONS
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