# -*- coding: utf-8 -*-
from django.conf import settings
from django.conf.urls import include
from django.conf.urls.static import static
from django.urls import path
from rest_framework.routers import DefaultRouter

from zeroindex.utils.admin import admin_site
from zeroindex.utils.views import healthz, eth_price

# Unused
api_router = DefaultRouter(trailing_slash=True)
api_router.include_root_view = settings.DEBUG
api_router.include_format_suffixes = False

urlpatterns = [
    path("api/", include(api_router.urls)),
    path("api/eth_price/", eth_price),
    path("mgmt/", admin_site.urls),
    path("healthz/", healthz),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)