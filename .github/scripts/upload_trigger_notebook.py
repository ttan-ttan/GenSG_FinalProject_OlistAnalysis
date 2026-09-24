"""Update the Trigger_Notebook Fabric item definition."""

import base64
import json
import os
from pathlib import Path

import msal
import requests

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
WORKSPACE_ID = os.environ["FABRIC_WORKSPACE_ID"]
NOTEBOOK_ID = os.environ["FABRIC_NOTEBOOK_ID"]
NOTEBOOK_PATH = Path("notebooks/Trigger_Notebook.notebook/notebook-content.json")


def get_access_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET,
    )
    token = app.acquire_token_for_client(
        scopes=["https://api.fabric.microsoft.com/.default"]
    )
    if "access_token" not in token:
        raise RuntimeError(f"Failed to acquire Fabric token: {token}")
    return token["access_token"]


def main():
    if not NOTEBOOK_PATH.exists():
        raise FileNotFoundError(f"Notebook file not found: {NOTEBOOK_PATH}")

    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    payload = base64.b64encode(
        json.dumps(notebook, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("ascii")

    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
        f"/notebooks/{NOTEBOOK_ID}/updateDefinition"
    )
    body = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": payload,
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {get_access_token()}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=60,
    )
    print(f"Fabric response: HTTP {response.status_code}")
    print(response.text)
    response.raise_for_status()


if __name__ == "__main__":
    main()
