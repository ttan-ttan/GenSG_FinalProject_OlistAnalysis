"""
Upload local src/ folder to Fabric OneLake Files/src using REST API.
This script is executed by GitHub Actions on every push to main.
"""

import os
import requests

FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
FABRIC_LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")
FABRIC_ACCESS_TOKEN = os.getenv("FABRIC_ACCESS_TOKEN")

SRC_FOLDER = "src"

BASE_URL = (
    f"https://api.fabric.microsoft.com/v1/workspaces/"
    f"{FABRIC_WORKSPACE_ID}/lakehouses/"
    f"{FABRIC_LAKEHOUSE_ID}/files/src"
)

HEADERS = {
    "Authorization": f"Bearer {FABRIC_ACCESS_TOKEN}"
}

# Upload each file inside src/
for root, dirs, files in os.walk(SRC_FOLDER):
    for file in files:
        local_path = os.path.join(root, file)

        # Build OneLake path
        relative_path = os.path.relpath(local_path, SRC_FOLDER)
        onelake_path = f"{BASE_URL}/{relative_path}?overwrite=true"

        with open(local_path, "rb") as f:
            response = requests.put(
                onelake_path,
                headers=HEADERS,
                data=f,
                timeout=30  # prevent hanging forever
            )

        print(f"Uploaded {relative_path}: {response.status_code}")
