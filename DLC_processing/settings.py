import os
from pathlib import Path
from celery.schedules import crontab

BASE_DIR = Path(__file__).resolve().parent.parent


SECRET_KEY = 'django-insecure-pmtr-d$sb_v#gg$fm^w)ihr!2t5bp5_b=%enaplyoc^)je71c%'
DEBUG = True


ALLOWED_HOSTS = ["*", "localhost", "127.0.0.1"]


INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    # 'django_filters',
    'accounts',

    'processor',
]


MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
    ],
    "DEFAULT_FILTER_BACKENDS": ["django_filters.rest_framework.DjangoFilterBackend"]

}

ROOT_URLCONF = 'DLC_processing.urls'


TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        # Project-level templates dir
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                # any other processors...
            ],
        },
    },
]

WSGI_APPLICATION = 'DLC_processing.wsgi.application'
LOGIN_REDIRECT_URL = '/dashboard/'   # change as you like
LOGOUT_REDIRECT_URL = '/login/'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('POSTGRES_DB', 'demand_db'),
        'USER': os.getenv('POSTGRES_USER', 'postgres'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD', 'Frcst$2025'),
        'HOST': os.getenv('POSTGRES_HOST', '172.16.7.119'),
        'PORT': int(os.getenv('POSTGRES_PORT', '8010')),
    }
}


AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]


LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Kolkata'
USE_I18N = True
USE_TZ = True


STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')


STATIC_DIR = os.path.join(BASE_DIR, "static")
if os.path.exists(STATIC_DIR):
    STATICFILES_DIRS = [STATIC_DIR]


DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://redis:6380/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6380/0')

CELERY_BEAT_SCHEDULE = {
    "capture-demand-every-5-minutes": {
        "task": "processor.tasks.capture_demand_data_task",
        "schedule": 300,
    },
    'run_management_commands_at_8am': {
        'task': 'processor.tasks.run_management_commands',
        'schedule': crontab(minute=0, hour=8),  # Runs daily at 8 AM
        'args': (['nrldc_project', 'posoco', 'srldc_project', 'wrldc_project', 'merge_reports'],),
    },
    'run_management_commands_at_11am': {
        'task': 'processor.tasks.run_management_commands',
        'schedule': crontab(minute=0, hour=11),  # Runs daily at 11 AM
        'args': (['nrldc_project', 'posoco', 'srldc_project', 'wrldc_project', 'merge_reports'],),
    },
}


LOGIN_REDIRECT_URL = '/dashboard/'  # change as you like
LOGIN_URL = '/login/'
