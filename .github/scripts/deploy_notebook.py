# After deploy.yml run, it will call this script first to Authenticate with fabric
# And deploy notebooks Bronze Silver Gold to Fabric.

import argparse
import base64
import msal
import requests
import os

parser = argparse.ArgumentParser()
parser.add_argument("--workspace", required=True)
parser.add_argument("--notebook", required=True)
parser.add_argument("--file", required=True)
args = parser.parse_args()

tenant_id = os.environ["TENANT_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

authority = f"https://login.microsoftonline.com/{tenant_id}"
app = msal.ConfidentialClientApplication(
    client_id=client_id,
    authority=authority,
    client_credential=client_secret,
)

token = app.acquire_token_for_client(
    scopes=["https://api.fabric.microsoft.com/.default"]
)
if "access_token" not in token:
    raise RuntimeError("Failed to acquire Fabric token")

access_token = token["access_token"]

with open(args.file, "rb") as f:
    encoded = base64.b64encode(f.read()).decode("ascii")

payload = {
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": os.path.basename(args.file),
                "payload": encoded,
                "payloadType": "InlineBase64"
            }
        ]
    }
}

url = (
    f"https://api.fabric.microsoft.com/v1/workspaces/"
    f"{args.workspace}/notebooks/{args.notebook}/updateDefinition"
)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

resp = requests.post(url, headers=headers, json=payload, timeout=60)
print("Status:", resp.status_code)
print("Response:", resp.text)
resp.raise_for_status()
