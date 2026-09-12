"""Update a Microsoft Fabric notebook from a checked-out ipynb file."""

import base64
import os
from pathlib import Path

import msal
import requests


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


tenant_id = required_environment("TENANT_ID")
client_id = required_environment("CLIENT_ID")
client_secret = required_environment("CLIENT_SECRET")
workspace_id = required_environment("FABRIC_WORKSPACE_ID")
notebook_id = required_environment("FABRIC_NOTEBOOK_ID")
notebook_path = Path(os.environ.get(
    "FABRIC_NOTEBOOK_PATH", "notebooks/test_deploy.ipynb"))

if not notebook_path.is_file():
    raise FileNotFoundError(f"Notebook not found: {notebook_path}")

authority = f"https://login.microsoftonline.com/{tenant_id}"
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    authority=authority,
    client_credential=client_secret,
)
token_result = app.acquire_token_for_client(
    scopes=["https://api.fabric.microsoft.com/.default"]
)
if "access_token" not in token_result:
    raise RuntimeError(
        f"Fabric authentication failed: {token_result.get('error_description', token_result)}")

payload = {
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": notebook_path.name,
                "payload": base64.b64encode(notebook_path.read_bytes()).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        ],
    }
}

url = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    f"/notebooks/{notebook_id}/updateDefinition"
)
headers = {
    "Authorization": f"Bearer {token_result['access_token']}",
    "Content-Type": "application/json",
}
response = requests.post(url, headers=headers, json=payload, timeout=60)
if response.status_code not in (200, 202):
    raise RuntimeError(
        f"Fabric notebook update failed ({response.status_code}): {response.text}")

if response.status_code == 202:
    operation_url = response.headers.get("Location")
    if not operation_url:
        raise RuntimeError(
            "Fabric accepted the update but returned no operation URL")

    operation = requests.get(operation_url, headers=headers, timeout=60)
    if operation.status_code != 200:
        raise RuntimeError(
            f"Fabric notebook update operation failed ({operation.status_code}): {operation.text}"
        )

print(f"Updated Fabric notebook {notebook_id} from {notebook_path}")
