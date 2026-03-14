import os

# ── Security ────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME_in_env_file")

# ── Feature flags ───────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,   # Jinja in SQL
    "DASHBOARD_NATIVE_FILTERS":   True,
    "ALERT_REPORTS":              False,
}

# ── Cache (simple in-memory; swap for Redis in prod) ────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# ── Prevent Superset from loading example datasets ──────────────────────────
SUPERSET_LOAD_EXAMPLES = False

# ── Row limit for ad-hoc queries ────────────────────────────────────────────
ROW_LIMIT = 50_000

# ── Prevent CSRF issues when running behind a reverse proxy ─────────────────
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False