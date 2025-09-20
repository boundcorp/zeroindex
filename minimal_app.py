#!/usr/bin/env python
"""
Minimal Django app for testing ingress
"""
from django.conf import settings
from django.core.management import execute_from_command_line
from django.http import JsonResponse
from django.urls import path
import sys

settings.configure(
    DEBUG=True,
    SECRET_KEY='development-secret-key',
    ROOT_URLCONF=__name__,
    ALLOWED_HOSTS=['*'],
    INSTALLED_APPS=[
        'django.contrib.contenttypes',
        'django.contrib.auth',
    ],
    MIDDLEWARE=[
        'django.middleware.common.CommonMiddleware',
    ],
)

def index(request):
    return JsonResponse({
        'status': 'ok',
        'message': 'ZeroIndex Django App',
        'host': request.get_host(),
        'path': request.path,
    })

urlpatterns = [
    path('', index),
    path('health/', index),
]

if __name__ == '__main__':
    execute_from_command_line(['manage.py', 'runserver', '0.0.0.0:8000'])