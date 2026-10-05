#!/usr/bin/env python
# fmt: off
# ruff: noqa
# pylint: skip-file
# flake8: noqa

import os
import sys
import glob
import json
import shutil
import winreg
import zipfile
import subprocess
import importlib.util
import pandas as pd
from deltalake import write_deltalake

# ---------------------------------------------------------
# HELPER: Refresh Environment Variables (In-Memory)
# ---------------------------------------------------------
def refresh_path_env():
    """Fetch updated PATH from Windows Registry so newly installed CLI tools work immediately."""
    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
            0,
            winreg.KEY_READ,
        ) as key:
            sys_path, _ = winreg.QueryValueEx(key, "Path")
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Environment",
            0,
            winreg.KEY_READ,
        ) as key:
            user_path, _ = winreg.QueryValueEx(key, "Path")

        os.environ["PATH"] = f"{sys_path};{user_path};{os.environ.get('PATH', '')}"
    except Exception:
        pass

# ---------------------------------------------------------
# STEP 0: Generate Fabric Notebook IPYNB (Full Audit + Download Card)
# ---------------------------------------------------------
def create_export_notebook_file():
    """Generates the Fabric export notebook file on Desktop with Audit Dashboard & Download Prompts."""
    notebook_content = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import os\n",
                    "import json\n",
                    "import time\n",
                    "import base64\n",
                    "import shutil\n",
                    "from datetime import datetime\n",
                    "from pyspark.sql import SparkSession\n",
                    "from IPython.display import display, HTML\n\n",
                    "spark = SparkSession.builder.getOrCreate()\n",
                    "start_time = time.time()\n\n",
                    "EXPORT_DIR = \"Files/export_all/\"\n",
                    "LOGS_DIR = f\"{EXPORT_DIR}audit_logs/\"\n",
                    "NOTEBOOKS_EXPORT_DIR = f\"{EXPORT_DIR}notebooks/\"\n",
                    "os.makedirs(LOGS_DIR, exist_ok=True)\n",
                    "os.makedirs(NOTEBOOKS_EXPORT_DIR, exist_ok=True)\n\n",
                    "# 1. Export Tables\n",
                    "tables, table_stats = [], []\n",
                    "try:\n",
                    "    tables = [t.name for t in spark.catalog.listTables()]\n",
                    "except Exception:\n",
                    "    print(\"Lakehouse not attached.\")\n",
                    "    raise Exception(\"Attach Lakehouse and retry.\")\n\n",
                    "for t in tables:\n",
                    "    df = spark.read.table(t)\n",
                    "    row_cnt, col_cnt = df.count(), len(df.columns)\n",
                    "    df.write.mode(\"overwrite\").parquet(f\"{EXPORT_DIR}{t}.parquet\")\n",
                    "    if \"dq_\" in t or \"log\" in t:\n",
                    "        df.toPandas().to_csv(f\"{LOGS_DIR}{t}.csv\", index=False)\n",
                    "    table_stats.append({\"table_name\": t, \"row_count\": row_cnt, \"column_count\": col_cnt})\n\n",
                    "# 2. Extract Semantic Models, Reports & Notebooks\n",
                    "semantic_meta, reports_list = {}, []\n",
                    "try:\n",
                    "    import sempy.fabric as fabric\n",
                    "    datasets = fabric.list_datasets()\n",
                    "    for _, row in datasets.iterrows():\n",
                    "        d_name, d_id = row[\"Dataset Name\"], row[\"Dataset ID\"]\n",
                    "        try:\n",
                    "            m_df, r_df = fabric.list_measures(d_id), fabric.list_relationships(d_id)\n",
                    "            semantic_meta[d_name] = {\n",
                    "                \"id\": d_id,\n",
                    "                \"measures\": m_df.to_dict(orient=\"records\") if not m_df.empty else [],\n",
                    "                \"relationships\": r_df.to_dict(orient=\"records\") if not r_df.empty else []\n",
                    "            }\n",
                    "        except Exception:\n",
                    "            pass\n",
                    "    with open(f\"{EXPORT_DIR}semantic_models.json\", \"w\", encoding=\"utf-8\") as f:\n",
                    "        json.dump(semantic_meta, f, indent=2)\n",
                    "    reports_df = fabric.list_reports()\n",
                    "    if not reports_df.empty:\n",
                    "        reports_list = reports_df[\"Report Name\"].tolist()\n",
                    "        with open(f\"{EXPORT_DIR}workspace_report_summary.json\", \"w\", encoding=\"utf-8\") as f:\n",
                    "            json.dump(reports_df.to_dict(orient=\"records\"), f, indent=2)\n",
                    "    notebooks_df = fabric.list_notebooks()\n",
                    "    if not notebooks_df.empty:\n",
                    "        for _, row in notebooks_df.iterrows():\n",
                    "            nb_name, nb_id = row[\"Name\"], row[\"ID\"]\n",
                    "            if \"Import_To_Fabric\" in nb_name or \"export\" in nb_name.lower():\n",
                    "                continue\n",
                    "            try:\n",
                    "                definition = fabric.get_notebook_definition(nb_id)\n",
                    "                parts = definition.get(\"definition\", {}).get(\"parts\", [])\n",
                    "                py_code = []\n",
                    "                for part in parts:\n",
                    "                    path, payload = part.get(\"path\", \"\"), part.get(\"payload\", \"\")\n",
                    "                    if path.endswith(\".ipynb\") or path.endswith(\".py\") or \"notebook-content\" in path:\n",
                    "                        decoded_str = base64.b64decode(payload).decode(\"utf-8\")\n",
                    "                        if path.endswith(\".ipynb\") or decoded_str.strip().startswith(\"{\"):\n",
                    "                            nb_json = json.loads(decoded_str)\n",
                    "                            for c in nb_json.get(\"cells\", []):\n",
                    "                                if c.get(\"cell_type\") == \"code\":\n",
                    "                                    source = c.get(\"source\", [])\n",
                    "                                    py_code.append(\"\".join(source) if isinstance(source, list) else source)\n",
                    "                                    py_code.append(\"\\n\\n# \" + \"-\"*50 + \"\\n\")\n",
                    "                        else:\n",
                    "                            py_code.append(decoded_str)\n",
                    "                if py_code:\n",
                    "                    clean_filename = f\"{nb_name}.py\".replace(\" \", \"_\")\n",
                    "                    with open(os.path.join(NOTEBOOKS_EXPORT_DIR, clean_filename), \"w\", encoding=\"utf-8\") as f:\n",
                    "                        f.write(f\"# Fabric Notebook Export: {nb_name}\\n\\n\" + \"\\n\".join(py_code))\n",
                    "            except Exception as nb_err:\n",
                    "                print(f\"Skipping {nb_name}: {nb_err}\")\n",
                    "except Exception as e:\n",
                    "    print(f\"Metadata Note: {e}\")\n\n",
                    "# 3. Build Session Log & Zip\n",
                    "exec_duration = round(time.time() - start_time, 2)\n",
                    "session_log = {\n",
                    "    \"execution_timestamp\": datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\"),\n",
                    "    \"spark_application_id\": spark.sparkContext.applicationId,\n",
                    "    \"total_execution_time_seconds\": exec_duration,\n",
                    "    \"total_tables_exported\": len(tables),\n",
                    "    \"table_metrics\": table_stats\n",
                    "}\n",
                    "with open(f\"{EXPORT_DIR}migration_audit_log.json\", \"w\", encoding=\"utf-8\") as f:\n",
                    "    json.dump(session_log, f, indent=2)\n\n",
                    "shutil.make_archive(\"Files/export_all\", 'zip', EXPORT_DIR)\n\n",
                    "# 4. Render Audit Summary + Download Instructions\n",
                    "r_str = \", \".join(reports_list) if reports_list else \"Power BI Reports detected\"\n",
                    "display(HTML(f\"\"\"\n",
                    "<div style='font-family: Arial, sans-serif; margin-top: 15px;'>\n",
                    "    <h2 style='color: #1a73e8;'>Fabric Migration & Activity Audit Summary</h2>\n",
                    "    <p style='color: #555; font-size: 13px;'><b>Executed On:</b> {session_log['execution_timestamp']} | <b>Duration:</b> {exec_duration}s | <b>Spark App ID:</b> {session_log['spark_application_id']}</p>\n",
                    "    <table style='width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; margin-top: 10px;'>\n",
                    "        <thead>\n",
                    "            <tr style='background-color: #f2f2f2; border-bottom: 2px solid #ccc;'>\n",
                    "                <th style='padding: 8px; border: 1px solid #ddd;'>Artifact Category</th>\n",
                    "                <th style='padding: 8px; border: 1px solid #ddd;'>Count / Details</th>\n",
                    "                <th style='padding: 8px; border: 1px solid #ddd;'>Status</th>\n",
                    "                <th style='padding: 8px; border: 1px solid #ddd;'>Target Output Format</th>\n",
                    "            </tr>\n",
                    "        </thead>\n",
                    "        <tbody>\n",
                    "            <tr>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><b>Lakehouse Data Tables</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'>{len(tables)} Tables</td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd; color: green;'><b>Automated</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'>Parquet / Delta Lake</td>\n",
                    "            </tr>\n",
                    "            <tr>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><b>Semantic Models (DAX)</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'>{len(semantic_meta)} Models</td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd; color: green;'><b>Automated</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><code>semantic_models.json</code></td>\n",
                    "            </tr>\n",
                    "            <tr>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><b>Power BI Reports</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'>{len(reports_list)} Reports ({r_str})</td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd; color: #d93025;'><b>Metadata Only</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><code>workspace_report_summary.json</code></td>\n",
                    "            </tr>\n",
                    "            <tr>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><b>Audit & Monitoring Logs</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'>Session Metrics + DQ Log Tables</td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd; color: green;'><b>Automated</b></td>\n",
                    "                <td style='padding: 8px; border: 1px solid #ddd;'><code>migration_audit_log.json</code> & <code>logs/</code></td>\n",
                    "            </tr>\n",
                    "        </tbody>\n",
                    "    </table>\n",
                    "    <br>\n",
                    "    <div style='background-color: #fff3cd; border: 2px solid #ffeba2; padding: 15px; border-radius: 8px; text-align: center;'>\n",
                    "        <h3 style='color: #856404; margin-top: 0;'>Export Successful! How to Download export_all.zip</h3>\n",
                    "        <div style='background-color: #ffffff; padding: 12px; border-radius: 5px; text-align: left; display: inline-block; border: 1px solid #ddd;'>\n",
                    "            <ol style='margin-bottom: 0; padding-left: 20px; font-size: 14px;'>\n",
                    "                <li>Click your <b>Lakehouse tab</b> at the top of the screen.</li>\n",
                    "                <li>In the left Explorer panel, expand <b>Files</b>.</li>\n",
                    "                <li>Right-click <b>export_all.zip</b> and click <b>Download</b>.</li>\n",
                    "                <li>Return to <code>FabricMigrationTool.exe</code> and press <b>ENTER</b>.</li>\n",
                    "            </ol>\n",
                    "        </div>\n",
                    "    </div>\n",
                    "</div>\n",
                    "\"\"\"))\n"
                ]
            }
        ],
        "metadata": {"language_info": {"name": "python"}},
        "nbformat": 4,
        "nbformat_minor": 2
    }

    desktop = os.path.expanduser("~/Desktop")
    filepath = os.path.join(desktop, "1_Import_To_Fabric.ipynb")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(notebook_content, f, indent=2)
    return filepath

# ---------------------------------------------------------
# STEP 1: GitHub CLI Handler
# ---------------------------------------------------------
def run_gh_login_and_wait():
    print("\n====================================================")
    print("  [!] Launching GitHub Login...")
    print("====================================================\n")
    subprocess.run("cmd /c gh auth login", shell=True)
    res = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True)
    return res.returncode == 0

def check_gh_cli():
    refresh_path_env()
    try:
        res = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True)
        if res.returncode != 0 and not run_gh_login_and_wait():
            print("\n[!] GitHub login incomplete.")
            return False
        return True
    except FileNotFoundError:
        print("\n====================================================")
        print("  [!] GitHub CLI ('gh') was not found on your system.")
        print("====================================================")
        choice = input("Automatically install GitHub CLI via winget now? (Y/n): ").strip().lower()
        if choice in ['', 'y', 'yes']:
            install_cmd = [
                "winget", "install", "--id", "GitHub.cli", "--source", "winget",
                "--accept-source-agreements", "--accept-package-agreements"
            ]
            if subprocess.run(install_cmd).returncode == 0:
                refresh_path_env()
                return run_gh_login_and_wait()
        return False

# ---------------------------------------------------------
# RETRY HELPERS FOR GIT / GH
# ---------------------------------------------------------
def run_cmd_retry(cmd_list_or_str, fail_msg):
    """Run a command until it succeeds, with ENTER checkpoints."""
    while True:
        if isinstance(cmd_list_or_str, list):
            result = subprocess.run(cmd_list_or_str)
        else:
            result = subprocess.run(cmd_list_or_str, shell=True)
        if result.returncode == 0:
            return True
        print(f"\n[!] {fail_msg}")
        input("Press ENTER to retry...")

# ---------------------------------------------------------
# MIGRATION PROCESS (Continuous Wizard + Full Metadata)
# ---------------------------------------------------------
def run_full_migration():
    """
    Single-run, continuous wizard:
    - ZIP detection loop + Fabric notebook generation if missing
    - Extract + map artifacts + Delta-RS conversion
    - Ask Bronze/Silver/Gold notebook names
    - Generate main.py (bound + interactive orchestrator)
    - Git init + commit + branch
    - GitHub repo creation (retry)
    - Git push (retry)
    - Final success message
    """
    downloads = os.path.expanduser("~/Downloads")

    # ZIP detection loop + Fabric notebook generation if missing
    while True:
        zips = glob.glob(os.path.join(downloads, "export_all*.zip"))
        if zips:
            break
        nb_path = create_export_notebook_file()
        print(f"\n[!] 'export_all.zip' missing in Downloads.\nCreated Notebook on Desktop: {nb_path}\n")
        input("Once 'export_all.zip' is downloaded, press ENTER to re-check Downloads...")

    zips.sort(key=os.path.getmtime, reverse=True)
    latest_zip = zips[0]
    print(f"\n[✓] Using export archive: {latest_zip}\n")

    # Target directory
    default_dir = os.getcwd()
    target_dir = input(f"Enter target directory path [Default: {default_dir}]: ").strip() or default_dir
    os.makedirs(target_dir, exist_ok=True)
    os.chdir(target_dir)

    # Repo name
    raw_repo_name = input("Enter GitHub repository name to create: ").strip()
    while not raw_repo_name:
        print("[ERROR] Repository name cannot be blank.")
        raw_repo_name = input("Enter GitHub repository name to create: ").strip()
    repo_name = raw_repo_name.replace(" ", "-")

    PROJECT = repo_name
    DATA = os.path.join(PROJECT, "data")
    BRONZE = os.path.join(DATA, "bronze")
    SILVER = os.path.join(DATA, "silver")
    GOLD = os.path.join(DATA, "gold")
    NOTEBOOKS = os.path.join(PROJECT, "notebooks")
    SEMANTIC = os.path.join(PROJECT, "semantic_models")
    REPORTS = os.path.join(PROJECT, "reports")
    AUDIT = os.path.join(PROJECT, "audit_and_logs")

    # Extract ZIP
    extract_folder = "export_all"
    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)
    with zipfile.ZipFile(latest_zip, 'r') as z:
        z.extractall(extract_folder)
    print("[1/6] ZIP extracted successfully.")

    # Create Directory Structure
    for folder in [PROJECT, DATA, BRONZE, SILVER, GOLD, NOTEBOOKS, SEMANTIC, REPORTS, AUDIT]:
        os.makedirs(folder, exist_ok=True)

    # .gitignore
    gitignore_path = os.path.join(PROJECT, ".gitignore")
    with open(gitignore_path, "w", encoding="utf-8") as f:
        f.write(
            "# Binaries & Executables\n*.exe\ndist/\nbuild/\n*.spec\n\n"
            "# Temporary Data Exports\nexport_all/\nexport_all*.zip\n*.parquet\n"
        )
    print("[1.1/6] Created .gitignore (excluding .exe and temporary exports).")

    # Map artifacts + Delta-RS conversion
    for root, dirs, files in os.walk(extract_folder):
        for file in files:
            src_path = os.path.join(root, file)

            if file.endswith(".py") and "notebooks" in root:
                dest = os.path.join(NOTEBOOKS, file)
                shutil.copy(src_path, dest)
                print(f"  [✓] Mapped Fabric Notebook: {file}")

            elif file.endswith(".parquet"):
                dest = os.path.join(BRONZE, file)
                shutil.copy(src_path, dest)
                df = pd.read_parquet(dest)
                delta_path = os.path.join(BRONZE, file.replace('.parquet', '_delta'))
                write_deltalake(delta_path, df)
                print(f"  [✓] Converted to Delta-RS: {file}")

            elif file == "semantic_models.json":
                shutil.copy(src_path, os.path.join(SEMANTIC, file))
                print("  [✓] Mapped Power BI Semantic Models (DAX definitions).")

            elif file == "workspace_report_summary.json":
                shutil.copy(src_path, os.path.join(REPORTS, file))
                print("  [✓] Mapped Workspace Report Catalog.")

            elif file == "migration_audit_log.json" or file.endswith(".csv"):
                shutil.copy(src_path, os.path.join(AUDIT, file))
                print(f"  [✓] Mapped Audit Log: {file}")

    print("[2/6] Datasets mapped to Bronze Delta Lake tables & metadata organized.")

    # Ask user for Bronze/Silver/Gold notebook names
    print("\n====================================================")
    print("  PIPELINE BINDING: BRONZE / SILVER / GOLD NOTEBOOKS")
    print("====================================================")
    bronze_nb = input("Enter Bronze notebook script name (without .py): ").strip()
    silver_nb = input("Enter Silver notebook script name (without .py): ").strip()
    gold_nb   = input("Enter Gold notebook script name   (without .py): ").strip()

    # Generate main.py that supports BOTH:
    # - bound Bronze/Silver/Gold run()
    # - interactive setup_and_run_orchestration()
    main_py_path = os.path.join(PROJECT, "main.py")
    main_py_content = f"""# Medallion ETL Pipeline Orchestrator (Bound + Interactive)

import os
import re
import importlib.util
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

NOTEBOOK_DIR = "notebooks"

def _exec_notebook(module_name):
    filename = module_name + ".py"
    filepath = os.path.join(NOTEBOOK_DIR, filename)
    if not os.path.exists(filepath):
        print(f"[ERROR] Notebook script '{{filename}}' not found in '{{NOTEBOOK_DIR}}'.")
        return
    logging.info(f"   --> Running: {{filename}}")
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "run"):
        module.run()
    elif hasattr(module, "main"):
        module.main()
    else:
        print(f"[WARN] Notebook '{{filename}}' has no 'run' or 'main' entrypoint.")

# --- Bound pipeline (user-selected Bronze/Silver/Gold) ---
def run_bronze():
    _exec_notebook("{bronze_nb}")

def run_silver():
    _exec_notebook("{silver_nb}")

def run_gold():
    _exec_notebook("{gold_nb}")

def run():
    print("\\n--- [BRONZE] ---")
    run_bronze()
    print("\\n--- [SILVER] ---")
    run_silver()
    print("\\n--- [GOLD] ---")
    run_gold()

# --- Interactive pipeline orchestrator (dynamic selection) ---
def setup_and_run_orchestration(notebook_dir=NOTEBOOK_DIR):
    if not os.path.exists(notebook_dir):
        print(f"[ERROR] Directory '{{notebook_dir}}' not found.")
        return

    all_files = sorted([f for f in os.listdir(notebook_dir) if f.endswith(".py") and not f.startswith("__")])

    if not all_files:
        print("[!] No notebook scripts found in notebooks/ directory.")
        return

    print("====================================================")
    print("      MEDALLION PIPELINE INTERACTIVE SETUP         ")
    print("====================================================\\n")

    def prompt_layer_selection(layer_name, keyword):
        matches = [f for f in all_files if re.search(r'\\b' + keyword + r'\\b|' + keyword + r'_|_' + keyword, f, re.IGNORECASE)]
        candidate_list = matches if matches else all_files

        print(f"\\n[!] Detected {{len(candidate_list)}} potential {{layer_name.upper()}} notebook(s):")
        for idx, f in enumerate(candidate_list, 1):
            print(f"  [{{idx}}] {{f}}")

        user_input = input(f"\\nConfirm notebook number(s) (separated by space, e.g. 1 2 3 7 8) to proceed as {{layer_name.upper()}} notebook(s) [Press ENTER to skip]: ").strip()

        selected_notebooks = []
        if user_input:
            indices = [int(x) for x in user_input.split() if x.isdigit()]
            for idx in indices:
                if 1 <= idx <= len(candidate_list):
                    selected_notebooks.append(candidate_list[idx - 1])

        return selected_notebooks

    selected_bronze = prompt_layer_selection("Bronze", "bronze")
    selected_silver = prompt_layer_selection("Silver", "silver")
    selected_gold   = prompt_layer_selection("Gold", "gold")

    print("\\n====================================================")
    print("          CONFIRMED PIPELINE ORCHESTRATION CHAIN    ")
    print("====================================================")
    print(f"  * BRONZE LAYER ({{len(selected_bronze)}}): {{selected_bronze if selected_bronze else 'None (Skipped)'}}")
    print(f"  * SILVER LAYER ({{len(selected_silver)}}): {{selected_silver if selected_silver else 'None (Skipped)'}}")
    print(f"  * GOLD LAYER   ({{len(selected_gold)}}):   {{selected_gold if selected_gold else 'None (Skipped)'}}")
    print("====================================================\\n")

    confirm = input("Proceed to execute this chained Medallion pipeline now? (Y/n): ").strip().lower()
    if confirm not in ['', 'y', 'yes']:
        print("\\n[!] Execution canceled by user.")
        return

    print("\\n====================================================")
    print("      STARTING MEDALLION PIPELINE EXECUTION         ")
    print("====================================================")

    if selected_bronze:
        logging.info("--- [STAGE 1/3] EXECUTING BRONZE LAYER PIPELINE ---")
        for nb in selected_bronze:
            _exec_notebook(nb.replace(".py", ""))

    if selected_silver:
        logging.info("--- [STAGE 2/3] EXECUTING SILVER LAYER PIPELINE ---")
        for nb in selected_silver:
            _exec_notebook(nb.replace(".py", ""))

    if selected_gold:
        logging.info("--- [STAGE 3/3] EXECUTING GOLD LAYER PIPELINE ---")
        for nb in selected_gold:
            _exec_notebook(nb.replace(".py", ""))

    print("\\n====================================================")
    print("      CHAINED PIPELINE EXECUTION SUCCESSFUL         ")
    print("====================================================")

if __name__ == "__main__":
    run()
"""
    with open(main_py_path, "w", encoding="utf-8") as f:
        f.write(main_py_content)
    print("[3/6] Generated main.py (bound Bronze/Silver/Gold + interactive orchestrator).")

    # Git + GitHub repo creation + push with retry loops
    print("[4/6] Initializing Git and publishing to GitHub...")
    os.chdir(PROJECT)

    run_cmd_retry(["git", "init"], "git init failed.")
    run_cmd_retry(["git", "add", "."], "git add failed.")
    run_cmd_retry(
        ["git", "commit", "-m", "Initial Fabric migration with Data, Notebooks, DAX, and Audit Logs"],
        "git commit failed.",
    )
    run_cmd_retry(["git", "branch", "-M", "main"], "git branch -M main failed.")

    # Ensure GitHub CLI + login
    if not check_gh_cli():
        print("\n[!] GitHub CLI or login not ready. Cannot proceed.")
        return PROJECT

    # Retry repo creation until success
    gh_create_cmd = ["gh", "repo", "create", repo_name, "--public", "--source=.", "--remote=origin"]
    run_cmd_retry(gh_create_cmd, "GitHub repository creation failed.")

    # Retry push until success
    run_cmd_retry(["git", "push", "-u", "origin", "main"], "Git push failed.")

    print("\n====================================================")
    print(f" SUCCESS! Repository '{repo_name}' is now published on GitHub.")
    print("====================================================")
    print("\nYou can now run your ETL locally with:")
    print(f"  cd {PROJECT}")
    print("  python main.py")
    print("\nWizard completed successfully. Returning to control center when you press ENTER.")
    input("\nPress ENTER to return to main menu...")
    return PROJECT

# ---------------------------------------------------------
# MASTER MENU CONTROL LOOP (IT-nerd friendly)
# ---------------------------------------------------------
def main():
    check_gh_cli()
    current_project = None

    while True:
        status_str = f" [Active Project: '{current_project}']" if current_project else ""

        print("\n====================================================")
        print(f"      MICROSOFT FABRIC MIGRATION CONTROL CENTER{status_str}")
        print("====================================================")
        print("  [1] Generate/Regenerate Fabric Export Notebook on Desktop")
        print("  [2] Run FULL Continuous Migration Wizard (ZIP → Delta-RS → GitHub)")
        print("  [3] Configure & Run ETL Pipeline (main.py: bound or interactive)")
        print("  [4] Re-initialize / Switch Repository Target")
        print("  [5] Exit")
        print("----------------------------------------------------")

        choice = input("Select an option (1, 2, 3, 4, or 5): ").strip()

        if choice == "1":
            nb_path = create_export_notebook_file()
            print(f"\n[✓] Fresh Fabric export notebook created on Desktop:\n    {nb_path}")
            input("\nPress ENTER to return to main menu...")

        elif choice == "2":
            project_dir = run_full_migration()
            if project_dir:
                current_project = project_dir

        elif choice == "3":
            main_path = "main.py" if os.path.exists("main.py") else (
                os.path.join(current_project, "main.py")
                if current_project and os.path.exists(os.path.join(current_project, "main.py"))
                else None
            )
            if main_path and os.path.exists(main_path):
                spec = importlib.util.spec_from_file_location("main", main_path)
                main_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(main_mod)
                # Prefer bound run(), fallback to interactive orchestrator
                if hasattr(main_mod, "run"):
                    main_mod.run()
                elif hasattr(main_mod, "setup_and_run_orchestration"):
                    main_mod.setup_and_run_orchestration()
                else:
                    print("\n[!] main.py has no 'run' or 'setup_and_run_orchestration'.")
            else:
                print("\n[!] 'main.py' not found. Please run Option [2] first to extract data and build the repository structure.")
            input("\nPress ENTER to return to main menu...")

        elif choice == "4":
            current_project = None
            print("\n[✓] Reset active project target. You can now extract a new ZIP or target a new directory.")
            input("\nPress ENTER to return to main menu...")

        elif choice == "5":
            print("\nExiting Fabric Migration Tool. Goodbye!")
            break

if __name__ == "__main__":
    main()

# Example PyInstaller command:
# pyinstaller --clean --onefile --name "FabricMigrationTool" --collect-all deltalake migration_tool.py
