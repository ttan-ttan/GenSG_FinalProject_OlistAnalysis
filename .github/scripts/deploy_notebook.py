"""
Deploy a Fabric notebook to OneLake using the Fabric REST API.
This script is executed by GitHub Actions to update notebooks automatically.
"""

import os
import requests

FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
FABRIC_LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")
FABRIC_ACCESS_TOKEN = os.getenv("FABRIC_ACCESS_TOKEN")

# Example constants (rename to uppercase for Pylint)
AUTHORITY = "api.fabric.microsoft.com"
BASE_URL = f"https://{AUTHORITY}/v1/workspaces/{FABRIC_WORKSPACE_ID}"

HEADERS = {
    "Authorization": f"Bearer {FABRIC_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


def deploy_notebook(notebook_path: str, notebook_name: str):
    """
    Upload a notebook file to Fabric Lakehouse Files/notebooks.
    """

    # Build upload URL
    url = (
        f"{BASE_URL}/lakehouses/"
        f"{FABRIC_LAKEHOUSE_ID}/files/notebooks/{notebook_name}?overwrite=true"
    )

    with open(notebook_path, "rb") as f:
        response = requests.put(
            url,
            headers=HEADERS,
            data=f,
            timeout=30  # prevent hanging forever
        )

    print(f"Uploaded notebook {notebook_name}: {response.status_code}")
    print(response.text)


if __name__ == "__main__":
    # Example usage
    deploy_notebook("notebooks/gold_customers.ipynb", "gold_customers.ipynb")
