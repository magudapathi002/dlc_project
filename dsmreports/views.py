from django.shortcuts import render

# Create your views here.
def dsm(request):
    return render(request, 'dsmreports/daily_dsm.html')
