"""
setup_secrets.py
One-time helper to store the Lakebase connection URL — and, optionally,
Adzuna/USAJobs API credentials for live job fetching — as Databricks
secrets. The deployed app (via app/lakebase.py) and etl/fetch_live_jobs.py
read these secrets at runtime through the `job_copilot` scope — no
credentials are ever committed to source control.

Run once, after provisioning the Lakebase instance and creating a
native-password role, from a terminal with Databricks auth configured
(or from a Databricks notebook):

    python setup_secrets.py

Safe to re-run: creating an already-existing scope is treated as a no-op,
and storing a secret under the same key overwrites the previous value.
Leave any API-key prompt blank to skip it if you're only using the static
files in samples/ rather than etl/fetch_live_jobs.py.
"""

import getpass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

# Must match LAKEBASE_SECRET_SCOPE / LAKEBASE_SECRET_KEY in app.yaml and lakebase.py.
SCOPE = "job_copilot"
LAKEBASE_KEY = "lakebase-url"

# Only needed if you're fetching live listings via etl/fetch_live_jobs.py
# instead of uploading the static files in samples/. RemoteOK's API is
# public and needs no key, so there's nothing to store for it.
ADZUNA_APP_ID_KEY = "adzuna-app-id"
ADZUNA_APP_KEY_KEY = "adzuna-app-key"
USAJOBS_AUTH_KEY_KEY = "usajobs-auth-key"
USAJOBS_EMAIL_KEY = "usajobs-email"

LAKEBASE_URL_PROMPT = (
    "Paste your Lakebase connection URL "
    "(postgresql://<role>:<password>@<host>.database.cloud.databricks.com:5432/"
    "databricks_postgres?sslmode=require): "
)


def ensure_scope(w: WorkspaceClient, scope: str) -> None:
    """Create the secret scope if it doesn't already exist."""
    try:
        w.secrets.create_scope(scope=scope)
        print(f"Created secret scope '{scope}'.")
    except Exception as e:
        print(f"Scope '{scope}' already exists (or could not be created): {e}")


def store_secret(w: WorkspaceClient, scope: str, key: str, prompt: str, hidden: bool = True) -> None:
    """
    Prompt for a value and store it as a secret.

    Stored as plain text via string_value — do NOT base64-encode this value
    yourself. The Databricks Secrets API always base64-encodes `value` on
    every get_secret() response regardless of how it was written; lakebase.py
    (and fetch_live_jobs.py) reverse that single encoding layer on read.
    Encoding here too would double-encode the value and break the reader.
    """
    value = getpass.getpass(prompt) if hidden else input(prompt)
    if not value:
        print(f"Skipped '{scope}/{key}' (blank input).")
        return
    w.secrets.put_secret(scope=scope, key=key, string_value=value)
    print(f"Stored secret '{scope}/{key}'.")


def grant_read_access(w: WorkspaceClient, scope: str) -> None:
    """Let the app's runtime identity (and other workspace users) read the secret."""
    w.secrets.put_acl(
        scope=scope, principal="users", permission=workspace.AclPermission.READ
    )
    print("Granted READ to 'users'.")


def main() -> None:
    w = WorkspaceClient()
    ensure_scope(w, SCOPE)

    store_secret(w, SCOPE, LAKEBASE_KEY, LAKEBASE_URL_PROMPT)

    print("\nOptional: live job fetching (etl/fetch_live_jobs.py) needs Adzuna "
          "and USAJobs credentials. Leave blank to skip if you're only using "
          "the static files in samples/.\n")
    store_secret(w, SCOPE, ADZUNA_APP_ID_KEY, "Adzuna app_id (blank to skip): ", hidden=False)
    store_secret(w, SCOPE, ADZUNA_APP_KEY_KEY, "Adzuna app_key (blank to skip): ")
    store_secret(w, SCOPE, USAJOBS_AUTH_KEY_KEY, "USAJobs Authorization-Key (blank to skip): ")
    store_secret(w, SCOPE, USAJOBS_EMAIL_KEY, "USAJobs registered email (blank to skip): ", hidden=False)

    grant_read_access(w, SCOPE)
    print("Done.")


if __name__ == "__main__":
    main()