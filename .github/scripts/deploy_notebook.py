"""
Deploy a Fabric notebook to OneLake using the Fabric REST API.

Purpose:
    This script is executed by GitHub Actions to automatically upload
    notebook JSON files into the Lakehouse Files/notebooks directory.
    It enables CI/CD deployment of Fabric notebooks without manual UI steps.

Notes:
    - This script requires the following environment variables:
        FABRIC_WORKSPACE_ID
        FABRIC_LAKEHOUSE_ID
        FABRIC_ACCESS_TOKEN
    - The GitHub Actions workflow passes these values securely.
    - The REST API endpoint supports overwrite=true to ensure updates
      replace existing notebook definitions.
    - Notebook files must follow the Fabric Git format:
        <NotebookName>.notebook/notebook-content.json
"""

import os
import sys
import requests

# ---------------------------------------------------------------------
# Environment variables provided by GitHub Actions
# ---------------------------------------------------------------------
FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
FABRIC_LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")
FABRIC_ACCESS_TOKEN = os.getenv("FABRIC_ACCESS_TOKEN")

AUTHORITY = "api.fabric.microsoft.com"
BASE_URL = f"https://{AUTHORITY}/v1/workspaces/{FABRIC_WORKSPACE_ID}"

HEADERS = {
    "Authorization": f"Bearer {FABRIC_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


def deploy_notebook(path: str, name: str):
    """
    Upload a Fabric notebook JSON file to OneLake.

    Args:
        path (str): Local filesystem path to notebook-content.json
        name (str): Target notebook name inside OneLake

    Raises:
        FileNotFoundError: If the notebook JSON file does not exist
        requests.exceptions.RequestException: For REST API failures
    """

    print(f"Deploying notebook: {path}")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Notebook file not found: {path}")

    url = (
        f"{BASE_URL}/lakehouses/"
        f"{FABRIC_LAKEHOUSE_ID}/files/notebooks/{name}?overwrite=true"
    )

    with open(path, "rb") as f:
        response = requests.put(
            url,
            headers=HEADERS,
            data=f,
            timeout=30
        )

    print(f"Uploaded notebook '{name}' → HTTP {response.status_code}")
    print(response.text)


# ---------------------------------------------------------------------
# CLI entry point (used by GitHub Actions)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError(
            "Usage: deploy_notebook.py <notebook_path> <notebook_name>"
        )

    # Avoid Pylint W0621 by using different variable names
    input_path = sys.argv[1]
    target_name = sys.argv[2]

    deploy_notebook(input_path, target_name)
