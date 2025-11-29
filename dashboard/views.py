# dashboard/views.py
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

SESSION_STATE_KEY = 'selected_state'

@login_required
def dashboard_view(request):
    selected_state = request.session.get(SESSION_STATE_KEY)
    context = {
        'user': request.user,
        'selected_state': selected_state,
    }
    return render(request, 'dashboard/dashboard.html', context)
