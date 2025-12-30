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
    
    # Default to showing all data (historical data) if no date is provided
    if not date:
        # If no date filter, show all historical data for all available dates
        a_tab = Srldc2AData.objects.all()  # Show all A data
        c_tab = Srldc2CData.objects.all()  # Show all C data
        b_tab = SRLDC3BData.objects.all()  # Show all B data

        return Response(
            {
                "requested_date": "all",
                "actual_report_date": "all",
                "table_a": SrldcASerializer(a_tab, many=True).data,  # ALL States
                "table_c": SrldcCSerializer(c_tab, many=True).data,  # ALL States
                "table_b": list(b_tab.values()),                     # ALL Stations
            },
            status=status.HTTP_200_OK
        )
    
    # ---------------- DATE PARSING ----------------
    try:
        requested_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return Response(
            {"error": "Invalid date format. Use YYYY-MM-DD"},
            status=status.HTTP_400_BAD_REQUEST
        )

    # ---------------- FETCH DATA FOR THE SPECIFIED DATE ----------------
    report_date = requested_date

    # Fetch data based on the requested date
    a_tab = Srldc2AData.objects.filter(report_date=report_date)
    c_tab = Srldc2CData.objects.filter(report_date=report_date)
    b_tab = SRLDC3BData.objects.filter(report_date=report_date)

    # If no data is found for the requested date, fallback to previous day
    if not a_tab.exists() and not c_tab.exists() and not b_tab.exists():
        report_date = requested_date - timedelta(days=1)
        a_tab = Srldc2AData.objects.filter(report_date=report_date)
        c_tab = Srldc2CData.objects.filter(report_date=report_date)
        b_tab = SRLDC3BData.objects.filter(report_date=report_date)

        # If no data is found for the previous day either
        if not a_tab.exists() and not c_tab.exists() and not b_tab.exists():
            return Response(
                {"error": f"No data available for the date {str(report_date)}"},
                status=status.HTTP_404_NOT_FOUND
            )

    # ---------------- RESPONSE ----------------
    return Response(
        {
            "requested_date": str(requested_date),
            "actual_report_date": str(report_date),
            "table_a": SrldcASerializer(a_tab, many=True).data,  # Filtered for the date
            "table_c": SrldcCSerializer(c_tab, many=True).data,  # Filtered for the date
            "table_b": list(b_tab.values()),                     # Filtered for the date
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