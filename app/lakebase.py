"""
lakebase.py
Runtime helper for connecting to Lakebase from the deployed Databricks App
(or any notebook/job running with Databricks auth configured).

Reads the connection URL written by setup_secrets.py from the workspace
secret store — the URL itself never appears in source control, app.yaml,
or environment variables checked into the repo.

Usage:
    from lakebase import get_connection

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
"""

import base64
import os

import psycopg
from psycopg.rows import dict_row
from databricks.sdk import WorkspaceClient

# Must match SCOPE / KEY in setup_secrets.py.
LAKEBASE_SECRET_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "job_copilot")
LAKEBASE_SECRET_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

_cached_url: str | None = None


def _fetch_connection_url() -> str:
    """
    Fetch and decode the Lakebase connection URL from the secret store.

    get_secret() always returns `value` base64-encoded regardless of how it
    was written (setup_secrets.py wrote it as plain text via string_value) —
    decode exactly once here. Do not re-encode/decode elsewhere.
    """
    global _cached_url
    if _cached_url is not None:
        return _cached_url

    w = WorkspaceClient()
    secret = w.secrets.get_secret(scope=LAKEBASE_SECRET_SCOPE, key=LAKEBASE_SECRET_KEY)
    _cached_url = base64.b64decode(secret.value).decode("utf-8")
    return _cached_url


def get_connection():
    """Return a new psycopg2 connection to Lakebase, using dict-like rows."""
    url = _fetch_connection_url()
    return psycopg.connect(url, row_factory=dict_row)
