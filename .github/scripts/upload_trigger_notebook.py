"""Update the Trigger_Notebook Fabric item definition."""

import base64
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import msal
import requests

TENANT_ID = os.environ["TENANT_ID"].strip()
CLIENT_ID = os.environ["CLIENT_ID"].strip()
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
WORKSPACE_ID = os.environ["FABRIC_WORKSPACE_ID"].strip()
NOTEBOOK_ID = os.environ["FABRIC_NOTEBOOK_ID"].strip()
NOTEBOOK_PATH = Path(
    "notebooks/trigger_pipeline.notebook/notebook-content.json")
UPLOAD_TIMESTAMP_TAG = "fabric-upload-timestamp"


def get_access_token():
    """Get an access token for the Fabric API using MSAL."""
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


def append_upload_timestamp(notebook):
    """Append one visible UTC upload timestamp to the notebook payload."""
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    cells = notebook.setdefault("cells", [])
    cells[:] = [
        cell
        for cell in cells
        if UPLOAD_TIMESTAMP_TAG not in cell.get("metadata", {}).get("tags", [])
    ]
    cells.append(
        {
            "cell_type": "markdown",
            "metadata": {
                "language": "markdown",
                "tags": [UPLOAD_TIMESTAMP_TAG],
            },
            "source": [f"**Last uploaded to Fabric (UTC):** `{timestamp}`\n"],
        }
    )
    return timestamp


def main():
    """Update the Trigger_Notebook Fabric item definition."""
    if not NOTEBOOK_PATH.exists():
        raise FileNotFoundError(f"Notebook file not found: {NOTEBOOK_PATH}")

    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    upload_timestamp = append_upload_timestamp(notebook)
    print(f"Uploading notebook with timestamp (UTC): {upload_timestamp}")
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
