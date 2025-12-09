# dailyreports/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('daily_reports/psp/', views.psp, name='psp'),
    path('daily_reports/error_report/', views.error_report, name='error_report'),
    path('daily_reports/monthly_error_report/', views.monthly_error_report, name='monthly_error_report'),
    path('daily_reports/daily_comparison/', views.daily_comparison, name='daily_comparison'),
    path('daily_reports/daily_windy_power/', views.daily_windy_power, name='daily_windy_power'),
    path('daily_reports/run_daily_dsm/', views.run_daily_dsm, name='run_daily_dsm'),
    path('daily_reports/accuracy_report/', views.accuracy_report, name='accuracy_report'),

]
