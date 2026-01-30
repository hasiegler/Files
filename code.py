import os
import re
from pathlib import Path

import numpy as np
import pandas as pd




# ============================================================
# HELPERS
# ============================================================
def extract_portfoliono(folder_name: str) -> int | None:
   
    m = re.match(r"^\s*(\d+)", folder_name)
    return int(m.group(1)) if m else None


def load_cot_data_from_folder(portfolio_folder: Path) -> pd.DataFrame:
    """
    Concatenate all Excel files in the folder that contain 'COT Data' in the filename.
    """
    dfs = []
    for f in portfolio_folder.iterdir():
        if f.is_file() and ("COT Data" in f.name) and (f.suffix.lower() in {".xlsx", ".xls"}):
            dfs.append(pd.read_excel(f))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def prep_portfolio_dfs(
    df_account_list_port: pd.DataFrame,
    df_chain_of_title: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Your cleanup/typing steps applied PER PORTFOLIO.
    Run this after loading df_chain_of_title and filtering df_account_list.
    """

    # ---- chain of title cleanup ----
    if "sold_date" in df_chain_of_title.columns:
        # replace literal "(null)" strings with nulls
        df_chain_of_title["sold_date"] = df_chain_of_title["sold_date"].replace("(null)", pd.NA)

        # then parse
        df_chain_of_title["sold_date"] = pd.to_datetime(
            df_chain_of_title["sold_date"],
            errors="coerce"
        )

    if "purchase_date" in df_chain_of_title.columns:
        df_chain_of_title["purchase_date"] = pd.to_datetime(
            df_chain_of_title["purchase_date"],
            errors="coerce"
        )

    # ---- account list typing ----
    acct_cast = {

    }
    acct_cast = {k: v for k, v in acct_cast.items() if k in df_account_list_port.columns}
    if acct_cast:
        df_account_list_port = df_account_list_port.astype(acct_cast)

    if "PlacementDate" in df_account_list_port.columns:
        df_account_list_port["PlacementDate"] = pd.to_datetime(
            df_account_list_port["PlacementDate"],
            errors="coerce"
        )

    # ---- chain of title typing ----
    cot_cast = {

    }
    cot_cast = {k: v for k, v in cot_cast.items() if k in df_chain_of_title.columns}
    if cot_cast:
        df_chain_of_title = df_chain_of_title.astype(cot_cast)

    return df_account_list_port, df_chain_of_title


# ============================================================
# MAIN
# ============================================================
df_account_list = pd.read_csv(ACCOUNT_LIST_PATH)

all_flags_master = []
errors = []

portfolio_folders = sorted([p for p in ROOT_DIR.iterdir() if p.is_dir()])

for portfolio_folder in portfolio_folders:
    port_no = extract_portfoliono(portfolio_folder.name)
    if port_no is None:
        # skip folders that don't start with digits
        continue

    # Filter account list to this portfolio number
    if PORTFOLIO_COL not in df_account_list.columns:
        raise KeyError(
            f"PORTFOLIO_COL='{PORTFOLIO_COL}' not found in AccountList. "
            f"Available columns: {list(df_account_list.columns)}"
        )

    df_account_list_port = df_account_list[df_account_list[PORTFOLIO_COL] == port_no].copy()
    if df_account_list_port.empty:
        print(f"SKIP (no accounts in AccountList for PortfolioNo={port_no}): {portfolio_folder.name}")
        continue

    # Load COT Data excels from this folder
    df_chain_of_title = load_cot_data_from_folder(portfolio_folder)
    if df_chain_of_title.empty:
        print(f"SKIP (no 'COT Data' Excel files found): {portfolio_folder.name}")
        continue

    # Apply your existing cleanup/typing (per portfolio)
    df_account_list_port, df_chain_of_title = prep_portfolio_dfs(df_account_list_port, df_chain_of_title)

    try:
        # ============================================================
        # YOUR EXISTING QC EXECUTION BLOCK GOES HERE (UNCHANGED)

        #
        # flags1 = qc_account_missing_from_cot(df_account_list_port, df_chain_of_title)
        # flags2 = qc_unique_account_count_mismatch(df_account_list_port, df_chain_of_title)
        # ...
        # allflags = pd.concat([flags1, flags2, ...], ignore_index=True)
        #
        # lookup = df_account_list_port[[]].copy() #add
        # all_flags_enriched = allflags.merge(lookup, on="", how="left")
        #
        # final_flags_df = all_flags_enriched
        # ============================================================

        final_flags_df = all_flags_enriched  # <-- CHANGE to your final df variable name

        # Write per-portfolio file
        out_path = OUTPUT_DIR / f"COT_QC_Flags_{port_no}.csv"
        final_flags_df.to_csv(out_path, index=False)

        # Keep for a master file too (optional)
        all_flags_master.append(final_flags_df.assign(PortfolioNo=port_no, PortfolioFolder=str(portfolio_folder)))

        print(f"DONE: {portfolio_folder.name} -> {out_path.name} (rows={len(final_flags_df):,})")

    except Exception as e:
        errors.append({"PortfolioNo": port_no, "Folder": str(portfolio_folder), "Error": str(e)})
        print(f"ERROR: {portfolio_folder.name}: {e}")


# Optional: write a master file with all portfolios combined
if all_flags_master:
    master = pd.concat(all_flags_master, ignore_index=True)
    master_path = OUTPUT_DIR / "COT_QC_Flags_ALL.csv"
    master.to_csv(master_path, index=False)
    print(f"\nWROTE MASTER: {master_path} (rows={len(master):,})")

# Optional: write an error log
if errors:
    err_df = pd.DataFrame(errors)
    err_path = OUTPUT_DIR / "COT_QC_Errors.csv"
    err_df.to_csv(err_path, index=False)
    print(f"WROTE ERRORS: {err_path} (count={len(errors):,})")

