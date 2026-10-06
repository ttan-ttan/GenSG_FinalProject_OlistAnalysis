import os

# Paths to your lists
fabric_path = "data/fabriclist.txt"
vsc_path = "data/vsclist.txt"

# -----------------------------
# 1. Load VS Code file list
# -----------------------------
with open(vsc_path, "r", encoding="utf-8") as f:
    vsc_files = {line.strip() for line in f if line.strip()}

# -----------------------------
# 2. Load Fabric file list
# -----------------------------
with open(fabric_path, "r", encoding="utf-8") as f:
    fabric_files = {line.strip() for line in f if line.strip()}

# -----------------------------
# 3. Normalize Fabric paths
# -----------------------------


def normalize_fabric(path):
    # Extract everything after /Files/
    if "/Files/" in path:
        return path.split("/Files/")[1]
    return path


fabric_normalized = {normalize_fabric(p) for p in fabric_files}

# -----------------------------
# 4. Compare sets
# -----------------------------
missing_in_fabric = sorted(vsc_files - fabric_normalized)
extra_in_fabric = sorted(fabric_normalized - vsc_files)

# -----------------------------
# 5. Output results
# -----------------------------
report = []

report.append(
    "=== Missing in Fabric (exists in VS Code but not in Fabric) ===")
report.extend(missing_in_fabric)

report.append(
    "\n=== Extra in Fabric (exists in Fabric but not in VS Code) ===")
report.extend(extra_in_fabric)

report.append("\n=== Summary ===")
report.append(f"VS Code files: {len(vsc_files)}")
report.append(f"Fabric files: {len(fabric_normalized)}")
report.append(f"Missing in Fabric: {len(missing_in_fabric)}")
report.append(f"Extra in Fabric: {len(extra_in_fabric)}")

# Write report
output_file = "compare_report.txt"
with open(output_file, "w", encoding="utf-8") as f:
    for line in report:
        f.write(str(line) + "\n")

print(f"Comparison complete. See {output_file}")
