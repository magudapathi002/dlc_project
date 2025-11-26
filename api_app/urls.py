from django.urls import path
from .views import srldc_view, nrldc_view, wrldc_view, posoco_view
from rest_framework.routers import DefaultRouter


urlpatterns =[
    path('srldc/', srldc_view, name='srldcapi'),
    path('nrldc/', nrldc_view, name='nrldcapi'),
    path('wrldc/', wrldc_view, name='nrldcapi'),
    path('posoco/', posoco_view, name='posocoapi'),

]

