import json
import os
from django.conf import settings
from django.shortcuts import render, redirect
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, get_user_model
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.contrib.auth.forms import AuthenticationForm
from django.views import View
from django.contrib.auth.views import LoginView
from django.http import JsonResponse

from .forms import SignUpForm

User = get_user_model()
SESSION_STATE_KEY = 'selected_state'


def _sanitize_state_name(raw: str):
    if not raw:
        return None
    return " ".join(raw.strip().split())


def _load_allowed_states():
    """Try to read allowed state names from static/data/india.geojson.
       Return a set of names (may be empty)."""
    geojson_path = os.path.join(settings.BASE_DIR, 'static', 'data', 'india.geojson')
    try:
        with open(geojson_path, 'r', encoding='utf8') as fh:
            data = json.load(fh)
        names = set()
        for feat in data.get('features', []):
            props = feat.get('properties', {}) or {}
            name = props.get('NAME_1') or props.get('NAME') or props.get('st_nm') or props.get('state')
            if name:
                names.add(name.strip())
        return names
    except Exception:
        return set()


ALLOWED_STATES = _load_allowed_states()


def signup_view(request):
    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save()
            messages.success(request, "Account created. You can log in now.")
            return redirect('login')
    else:
        form = SignUpForm()
    # NOTE: template changed to top-level 'signup.html' because your templates folder is project-level
    return render(request, 'accounts/signup.html', {'form': form})


def login_view(request):
    """
    Function-based login. Keeps existing behaviour but also injects
    selected_state from session into the template context for initial display.
    """
    next_url = request.GET.get('next') or request.POST.get('next') or None
    if isinstance(next_url, str) and next_url.lower() == 'none':
        next_url = None

    if request.method == 'POST':
        form = AuthenticationForm(request, data=request.POST)
        try:
            form.fields['username'].widget.attrs.update({
                'class': 'py-2.5 px-4 block w-full border border-gray-200 rounded-lg sm:text-sm focus:border-blue-500 focus:ring-blue-500',
                'placeholder': 'example@example.com'
            })
            form.fields['password'].widget.attrs.update({
                'class': 'py-2.5 px-4 block w-full border border-gray-200 rounded-lg sm:text-sm focus:border-blue-500 focus:ring-blue-500',
                'placeholder': 'Your Password'
            })
        except Exception:
            pass

        if form.is_valid():
            user = form.get_user()
            login(request, user)
            messages.success(request, f"Welcome back, {user.username}!")

            if next_url:
                allowed = url_has_allowed_host_and_scheme(
                    url=next_url,
                    allowed_hosts={request.get_host(), *getattr(settings, 'ALLOWED_HOSTS', [])},
                    require_https=request.is_secure(),
                )
                if allowed:
                    return redirect(next_url)

            return redirect('dashboard:dashboard')
        else:
            messages.error(request, "Invalid credentials. Try again.")
    else:
        form = AuthenticationForm()
        try:
            form.fields['username'].widget.attrs.update({
                'class': 'py-2.5 px-4 block w-full border border-gray-200 rounded-lg sm:text-sm focus:border-blue-500 focus:ring-blue-500',
                'placeholder': 'example@example.com'
            })
            form.fields['password'].widget.attrs.update({
                'class': 'py-2.5 px-4 block w-full border border-gray-200 rounded-lg sm:text-sm focus:border-blue-500 focus:ring-blue-500',
                'placeholder': 'Your Password'
            })
        except Exception:
            pass

    selected_state = request.session.get(SESSION_STATE_KEY)

    # NOTE: render top-level template file 'login.html'
    return render(request, 'accounts/login.html', {
        'form': form,
        'next': next_url,
        'selected_state': selected_state,
    })


@require_POST
def logout_view(request):
    """Logout (POST-only) and clear selected_state from session."""
    try:
        request.session.pop(SESSION_STATE_KEY, None)
    except Exception:
        pass
    logout(request)
    messages.info(request, "You have been logged out.")
    return redirect('login')


def select_state(request):
    """
    Accept AJAX POST to save `selected_state` in session and return JSON (no redirect).
    For non-AJAX calls it falls back to redirecting to login preserving a safe `next`.
    """
    raw_state = request.POST.get('state') or request.GET.get('state')
    state = _sanitize_state_name(raw_state) if raw_state else None

    if state and ALLOWED_STATES:
        if state not in ALLOWED_STATES:
            state = None

    if state:
        request.session[SESSION_STATE_KEY] = state

    is_ajax = request.headers.get('x-requested-with') == 'XMLHttpRequest'
    if is_ajax:
        return JsonResponse({'ok': True, 'state': state or ''})

    requested_next = request.GET.get('next') or request.POST.get('next') or reverse('dashboard:dashboard')
    try:
        is_safe = url_has_allowed_host_and_scheme(
            url=requested_next,
            allowed_hosts={request.get_host(), *getattr(settings, 'ALLOWED_HOSTS', [])},
            require_https=request.is_secure(),
        )
    except Exception:
        is_safe = False

    if not is_safe:
        requested_next = reverse('dashboard:dashboard')

    login_url = reverse('login')
    return redirect(f"{login_url}?next={requested_next}")


# Optional class-based login if you want to use Django's LoginView instead:
class StateAwareLoginView(LoginView):
    template_name = 'login.html'

    def dispatch(self, request, *args, **kwargs):
        state = request.GET.get('state')
        if state:
            s = _sanitize_state_name(state)
            if ALLOWED_STATES:
                if s in ALLOWED_STATES:
                    request.session[SESSION_STATE_KEY] = s
            else:
                request.session[SESSION_STATE_KEY] = s
        return super().dispatch(request, *args, **kwargs)
