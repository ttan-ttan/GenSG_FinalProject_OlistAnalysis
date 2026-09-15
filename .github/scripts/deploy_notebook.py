"""
Deploy a Fabric notebook to OneLake using the Fabric REST API.
This script is executed by GitHub Actions to update notebooks automatically.
"""

import os
import sys
import requests

FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
FABRIC_LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")
FABRIC_ACCESS_TOKEN = os.getenv("FABRIC_ACCESS_TOKEN")

AUTHORITY = "api.fabric.microsoft.com"
BASE_URL = f"https://{AUTHORITY}/v1/workspaces/{FABRIC_WORKSPACE_ID}"

HEADERS = {
    "Authorization": f"Bearer {FABRIC_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


def deploy_notebook(notebook_path: str, notebook_name: str):
    """
    Upload a Fabric notebook JSON file to Lakehouse Files/notebooks.
    """

    print(f"Deploying: {notebook_path}")

    if not os.path.exists(notebook_path):
        raise FileNotFoundError(f"Notebook file not found: {notebook_path}")

    url = (
        f"{BASE_URL}/lakehouses/"
        f"{FABRIC_LAKEHOUSE_ID}/files/notebooks/{notebook_name}?overwrite=true"
    )

    with open(notebook_path, "rb") as f:
        response = requests.put(
            url,
            headers=HEADERS,
            data=f,
            timeout=30
        )

    print(f"Uploaded notebook {notebook_name}: {response.status_code}")
    print(response.text)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError(
            "Usage: deploy_notebook.py <notebook_path> <notebook_name>")

    notebook_path = sys.argv[1]
    notebook_name = sys.argv[2]

    deploy_notebook(notebook_path, notebook_name)
