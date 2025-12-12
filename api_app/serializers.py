from rest_framework import serializers
from processor.models import Srldc2AData, Srldc2CData ,SRLDC3BData, Nrldc2AData, Nrldc2CData, Wrldc2CData, Wrldc2AData, PosocoTableG, PosocoTableA


class SrldcASerializer(serializers.ModelSerializer):
    class Meta:
        model = Srldc2AData
        fields = '__all__'


class SrldcCSerializer(serializers.ModelSerializer):
    class Meta:
        model = Srldc2CData
        fields = '__all__'

# class ScrldcBSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = SRLDC3BData
#         fields = '__all__'



class NrldcASerializer(serializers.ModelSerializer):
    class Meta:
        model = Nrldc2AData
        fields = '__all__'


class NrldcCSerializer(serializers.ModelSerializer):
    class Meta:
        model = Nrldc2CData
        fields = '__all__'

class WrldcASerializer(serializers.ModelSerializer):
    class Meta:
        model = Wrldc2AData
        fields = '__all__'

class WrldcCSerializer(serializers.ModelSerializer):
    class Meta:
        model = Wrldc2CData
        fields = '__all__'


class PosocoASerializer(serializers.ModelSerializer):
    class Meta:
        model = PosocoTableA
        fields = '__all__'


class PosocoGSerializer(serializers.ModelSerializer):
    class Meta:
        model = PosocoTableG
        fields = '__all__'
