from django.urls import path
from . import views

urlpatterns = [
    path('signup/', views.signup_view, name='signup'),
    path('', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    # path('dashboard/', views.dashboard_view, name='dashboard'),
    path('select_state/', views.select_state, name='select_state'),
]
