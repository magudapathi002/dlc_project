# views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from processor.models import Srldc2AData, Srldc2CData, Nrldc2CData, Nrldc2AData, Wrldc2AData, Wrldc2CData, PosocoTableA, PosocoTableG
from .serializers import SrldcASerializer, SrldcCSerializer, NrldcASerializer, NrldcCSerializer, WrldcASerializer, WrldcCSerializer, PosocoGSerializer, PosocoASerializer


@api_view(['GET'])
@permission_classes([AllowAny])
def srldc_view(request):
    a_tab = Srldc2AData.objects.all()
    c_tab = Srldc2CData.objects.all()
    return Response({
        "table_a": SrldcASerializer(a_tab, many=True).data,
        "table_c": SrldcCSerializer(c_tab, many=True).data
    }, status=status.HTTP_200_OK)

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