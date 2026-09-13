import os
import json

datasets = [
    "Customers",
    "Orders",
    "OrderItems",
    "Products",
    "Sellers",
    "Geolocation",
    "Payments",
    "Reviews"
]

layers = ["Bronze", "Silver", "Gold"]

base_dir = "notebooks"

template = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "cells": [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["# Auto-generated notebook\n"]
        }
    ],
    "metadata": {}
}

for dataset in datasets:
    for layer in layers:
        folder_name = f"{layer}_{dataset}.notebook"
        folder_path = os.path.join(base_dir, folder_name)

        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, "notebook-content.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(template, f, indent=4)

print("All notebook folders and files generated successfully.")
