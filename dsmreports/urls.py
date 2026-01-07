from django.urls import path
from . import views

urlpatterns = [
    path('dsmreports/daily_dsm/', views.dsm, name='daily_dsm'),
]
