# -*- coding: utf-8 -*-
import os
from datetime import timedelta
from typing import List, Tuple
from urllib.parse import urlparse

import environ


class Environments:
    PRODUCTION = "production"
    STAGING = "staging"
    DEVELOPMENT = "development"  # local development environment


def env_variable_truthy(key, default=""):
    return os.environ.get(key, default).lower().strip() in ["1", "true", "t", "y"]


env = environ.Env(
    DEBUG=(bool, False),
)  # set default values and casting
ENVIRONMENT = os.environ.get("APP_ENV", Environments.DEVELOPMENT).lower()
BACKEND_PORT = int(os.environ.get("DEVELOP_BACKEND_PORT", 8000))
INGRESS_PORT = int(os.environ.get("DEVELOP_INGRESS_PORT", 8888))
DEBUG = os.environ.get("DEBUG", "") == "true"
ROOT_URLCONF = "zeroindex.settings.urls"



DATABASE_HOST = os.environ.get("DATABASE_HOST", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "zeroindex")
DATABASE_USER = os.environ.get("DATABASE_USER", "zeroindex")
DATABASE_PORT = os.environ.get("DATABASE_PORT", "5432")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD", "zeroindex")
DATABASES = {
    # To enable postgis engine:
    # engine="django.contrib.gis.db.backends.postgis",
    "default": DATABASE_HOST
    and {
        "ENGINE": "django.db.backends.postgresql",
        "USER": DATABASE_USER,
        "NAME": DATABASE_NAME,
        "PASSWORD": DATABASE_PASSWORD,
        "HOST": DATABASE_HOST,
        "PORT": DATABASE_PORT,
    }
    or env.db("DATABASE_URL", default="")
}

MANAGERS = ADMINS = [
    ("Leeward Bound", "leeward@boundcorp.net"),
]

APP_HOSTNAME = os.environ.get("APP_HOSTNAME", "zeroindex.boundcorp.net")

SERVER_EMAIL = os.environ.get("SERVER_EMAIL", "noreply@" + APP_HOSTNAME)
DEFAULT_FROM_EMAIL = SERVER_EMAIL

AUTH_USER_MODEL = "users.User"
AUTHENTICATION_BACKENDS = ["django.contrib.auth.backends.ModelBackend"]

INSTALLED_APPS = [
    "django.contrib.auth",
    "corsheaders",
    "django.contrib.admin",
    "django.contrib.contenttypes",
    "django.contrib.humanize",
    "django.contrib.sessions",
    "django.contrib.sites",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "django_filters",
    "django_extensions",
    "zeroindex.apps.users",
]

SITE_ROOT = PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
BACKEND_FOLDER = SITE_ROOT + "/zeroindex"


def root(*x):
    return os.path.join(os.path.abspath(PROJECT_ROOT), *x)


PROJECT_MODULE = SITE_ROOT.split("/")[-1]

SERVE_MEDIA = True
USE_TZ = True

# BELOW IS CONFUSING!
# MEDIA_{ROOT,URL} -> User generated content
MEDIA_ROOT = root("static", "uploads")
MEDIA_URL = "/dj-static/uploads/"

# STATIC_{ROOT,URL} -> Python-collected static content
STATIC_ROOT = root("static", "assets")
STATIC_URL = "/dj-static/assets/"

# Where to collect ^above^ from:
STATICFILES_DIRS: List[Tuple[str, str]] = []

# Where the admin stuff lives
ADMIN_MEDIA_PREFIX = "/dj-static/assets/admin/"

# django-mediagenerator search directories
# files are defined in assets.py
GLOBAL_MEDIA_DIRS: List[str] = []

TIME_ZONE = "UTC"
LANGUAGE_CODE = "en-us"

INTERNAL_IPS = ["127.0.0.1", "10.0.2.2"]

IS_TEST = False

SITE_ID = 1

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": ("%(levelname)s %(asctime)s |" "%(pathname)s:%(lineno)d (in %(funcName)s) |" " %(message)s ")
        },
        "simple": {"format": "%(levelname)s %(message)s"},
    },
    "filters": {
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        }
    },
    "handlers": {
        "mail_admins": {
            "level": "ERROR",
            "class": "django.utils.log.AdminEmailHandler",
            "filters": ["require_debug_false"],
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "null": {
            "class": "logging.NullHandler",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": "WARNING",
        },
        "django.request": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": True,
        },
        "django.security.DisallowedHost": {
            "handlers": ["null"],
            "propagate": False,
        },
        "zeroindex": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
}

TEST_EXCLUDE = ("django",)
FIXTURE_DIRS = [
    root(PROJECT_ROOT, "fixtures"),
]

BASE_DIR = root(PROJECT_ROOT)

MESSAGE_STORAGE = "django.contrib.messages.storage.session.SessionStorage"

EMAIL_DEBUG = DEBUG

ACCOUNT_OPEN_SIGNUP = False

CORS_ORIGIN_ALLOW_ALL = DEBUG

CORS_ORIGIN_WHITELIST = ("http://localhost:8833",)
CORS_ALLOW_CREDENTIALS = True
from corsheaders.defaults import default_headers

CORS_ALLOW_HEADERS = list(default_headers) + [
    "sentry-trace",
]

SECRET_KEY = os.environ.get("SECRET_KEY", "secret")

WHITENOISE_MANIFEST_STRICT = False

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

TEMPLATES = [
    {
        # See:
        # https://docs.djangoproject.com/en/dev/ref/settings/#std:setting-TEMPLATES-BACKEND
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        # See:
        # https://docs.djangoproject.com/en/dev/ref/settings/#template-dirs
        "DIRS": [
            str(SITE_ROOT + "/templates"),
            str(BACKEND_FOLDER + "/templates"),
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            # See:
            # https://docs.djangoproject.com/en/dev/ref/settings/#template-debug
            "debug": DEBUG,
            # See: https://docs.djangoproject.com/en/dev/ref/settings/#template-loaders
            # https://docs.djangoproject.com/en/dev/ref/templates/api/#loader-types
            # 'loaders': [
            #     'django.template.loaders.filesystem.Loader',
            #     'django.template.loaders.app_directories.Loader',
            # ],
            # See: https://docs.djangoproject.com/en/dev/ref/settings/#template-context-processors
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.template.context_processors.i18n",
                "django.template.context_processors.media",
                "django.template.context_processors.static",
                "django.template.context_processors.tz",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

ALLOWED_HOSTS = ["127.0.0.1", "localhost", "*"]

# Use local filesystem for file storage (simplified from cloud storage)
DEFAULT_FILE_STORAGE = "django.core.files.storage.FileSystemStorage"

if not DEBUG:
    # STAGING AND PROD
    PROTOCOL = "https"
    FRONTEND_PORT = 80
    BASE_URL = os.environ.get("BASE_URL", f"{PROTOCOL}://{APP_HOSTNAME}")
    EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
    EMAIL_HOST = os.environ.get("SMTP_HOST", "")
    EMAIL_PORT = os.environ.get("SMTP_PORT", "")
    EMAIL_HOST_USER = os.environ.get("SMTP_USERNAME", "")
    EMAIL_HOST_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
    EMAIL_USE_TLS = os.environ.get("EMAIL_USE_TLS", "") == "true"
    BACKEND_URL = os.environ.get("BACKEND_URL", BASE_URL)

else:
    # DEV SETTINGS
    PROTOCOL = "http"
    FRONTEND_PORT = INGRESS_PORT
    APP_HOSTNAME = "localhost"
    BASE_URL = f"{PROTOCOL}://localhost:{FRONTEND_PORT}"
    EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
    BACKEND_URL = os.environ.get("BACKEND_URL", f"{PROTOCOL}://localhost:{BACKEND_PORT}")

APPEND_SLASH = True

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

if "SENTRY_BACKEND_URL" in os.environ:
    import sentry_sdk
    from sentry_sdk.integrations.django import DjangoIntegration

    sentry_sdk.init(
        dsn=os.environ["SENTRY_BACKEND_URL"],
        integrations=[DjangoIntegration()],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=0.1,
        # If you wish to associate users to errors (assuming you are using
        # django.contrib.auth) you may enable sending PII data.
        send_default_pii=True,
    )

THUMBNAIL_STORAGE = DEFAULT_FILE_STORAGE

# Redis configuration for localhost development
if "REDIS_URL" in os.environ:
    redis_url = urlparse(os.environ["REDIS_URL"])
    THUMBNAIL_REDIS_DB = "16"
    THUMBNAIL_REDIS_PASSWORD = redis_url.password or ""
    THUMBNAIL_REDIS_HOST = redis_url.hostname
    THUMBNAIL_REDIS_PORT = redis_url.port or 6379
else:
    # Default to localhost Redis
    THUMBNAIL_REDIS_DB = "16"
    THUMBNAIL_REDIS_PASSWORD = ""
    THUMBNAIL_REDIS_HOST = "localhost"
    THUMBNAIL_REDIS_PORT = 6379
