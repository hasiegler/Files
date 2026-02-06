import shutil
from pathlib import Path
import pandas as pd


# -------------------- CONFIG --------------------
PORTFOLIO_CSV = Path(r"C:\path\to\data_portfolio.csv")
MAPPING_CSV   = Path(r"C:\path\to\company_folder_map.csv")

PURCHASES_ROOT = Path(r"G:\_Purchases")
OUTPUT_ROOT    = Path(r"F:\Closing Statement Output")

DRY_RUN = False
# ------------------------------------------------


def load_mapping():
    df = pd.read_csv(MAPPING_CSV).fillna("")
    return {
        row["csv_company_name"].strip().lower(): {
            "folder": row["folder_name"].strip(),
            "subfolder": row["subfolder"].strip()
        }
        for _, row in df.iterrows()
    }


def find_closing_pdf(docs_path: Path):
    for f in docs_path.glob("*.pdf"):
        if "closing" in f.name.lower():
            return f
    return None


def main():
    OUTPUT_ROOT.mkdir(exist_ok=True)
    mapping = load_mapping()
    df = pd.read_csv(PORTFOLIO_CSV).fillna("")

    results = []

    for _, row in df.iterrows():
        company = row["Company Name (Clean)"]
        idl     = str(row["IDL Portfolio ID"])
        port    = str(row["Portfolio #"])

        key = company.lower().strip()
        if key not in mapping:
            results.append((company, idl, port, "NO_MAPPING"))
            continue

        m = mapping[key]

        # Build path to IDL folders
        idl_root = PURCHASES_ROOT / m["folder"]
        if m["subfolder"]:
            idl_root = idl_root / m["subfolder"]

        idl_folder = idl_root / f"IDL_{idl} ({port})"
        docs = idl_folder / "Docs"

        if not docs.exists():
            results.append((company, idl, port, "DOCS_NOT_FOUND"))
            continue

        pdf = find_closing_pdf(docs)
        if not pdf:
            results.append((company, idl, port, "NO_CLOSING_PDF"))
            continue

        out_dir = OUTPUT_ROOT / m["folder"]
        out_dir.mkdir(exist_ok=True)

        out_file = out_dir / pdf.name

        if not DRY_RUN:
            shutil.copy2(pdf, out_file)

        results.append((company, idl, port, "COPIED"))

    # Summary
    summary = pd.DataFrame(results, columns=["Company", "IDL", "Portfolio", "Status"])
    print(summary["Status"].value_counts())


if __name__ == "__main__":
    main()
