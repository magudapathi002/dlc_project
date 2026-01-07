"""
URL configuration for DLC_processing project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
# project urls.py (replace only the dashboard include line)
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('accounts.urls')),         # accounts handles /login /signup etc.
    path('dashboard/', include(('dashboard.urls', 'dashboard'), namespace='dashboard')),  # <- updated
    path('reports/', include('dailyreports.urls')), # moved dailyreports to /reports/ to avoid conflict
    path('api/', include('api_app.urls')),
    path('reports/', include('dsmreports.urls')), # moved dailyreports to /reports/ to avoid conflict
]
