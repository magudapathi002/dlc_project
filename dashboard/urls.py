# dashboard/urls.py
from django.urls import path
from . import views

app_name = 'dashboard'   # <-- add this to enable namespacing

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),   # global name inside the 'dashboard' namespace
]
