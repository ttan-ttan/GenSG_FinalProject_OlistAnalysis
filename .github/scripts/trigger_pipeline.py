"""
Trigger a Fabric pipeline using REST API.
"""

import os
import msal
import requests

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
WORKSPACE_ID = os.environ["FABRIC_WORKSPACE_ID"]
PIPELINE_ID = os.environ["FABRIC_PIPELINE_ID"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://api.fabric.microsoft.com/.default"]

app = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET,
)

token = app.acquire_token_for_client(scopes=SCOPE)
if "access_token" not in token:
    raise RuntimeError("Failed to acquire Fabric token")

access_token = token["access_token"]

url = (
    f"https://api.fabric.microsoft.com/v1/workspaces/"
    f"{WORKSPACE_ID}/pipelines/{PIPELINE_ID}/run"
)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

resp = requests.post(url, headers=headers, timeout=30)
print("Status:", resp.status_code)
print("Response:", resp.text)
resp.raise_for_status()
