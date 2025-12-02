import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Talkdesk Activity Case Relation data
SOURCE_FILE = r"D:\Production\Talkdesk_Activity_Case_Relation\SOURCE_FILE.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\All_Case_Destination_Org_v1_3035227.csv"
TALKDESK_ACTIVITY_LOOKUP_FILE = r"D:\Production\Lkp Files\TALKDESK_ACTIVITY_LOOKUP_FILE.csv"

# Output directory for audit reports
OUTPUT_DIR = r"D:\Production\Talkdesk_Activity_Case_Relation\Audit"

# ========= CONSTANTS =========
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"
CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_user_lookup(path):
    """Load user lookup file and return dictionary"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"User lookup file not found: {path}")
    
    df = pd.read_csv(path, dtype=str).fillna("")
    
    if "Legacy_SF_Record_ID__c" not in df.columns or "Id" not in df.columns:
        raise ValueError(f"User lookup file must contain 'Legacy_SF_Record_ID__c' and 'Id' columns")
    
    df["Legacy_SF_Record_ID__c"] = df["Legacy_SF_Record_ID__c"].astype(str).str.strip()
    df["Id"] = df["Id"].astype(str).str.strip()
    
    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df["Legacy_SF_Record_ID__c"], df["Id"])
        if str(k).strip()
    }


def load_lookup_dict(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a simple key-value lookup file"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lookup file not found: {path}")

    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    df = df.fillna("")

    missing = {key_col, value_col} - set(df.columns)
    if missing:
        raise ValueError(f"Missing column(s) in {path}: {', '.join(missing)}")

    # Strip whitespace and build lowercase key dictionary
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df[key_col], df[value_col])
        if str(k).strip()
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    print("📖 Loading lookup files...")
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Case lookup...")
    dict_case = load_lookup_dict(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(dict_case)} case mappings")
    
    print("   • Talkdesk Activity lookup...")
    dict_talkdesk_activity = load_lookup_dict(TALKDESK_ACTIVITY_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(dict_talkdesk_activity)} talkdesk activity mappings")

    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_DetailReport.csv")
    summary_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_SummaryReport.csv")

    # Remove existing output files
    for f in [detail_report_file, summary_report_file]:
        if os.path.exists(f):
            os.remove(f)

    # Columns to audit
    user_columns = ["CreatedById", "LastModifiedById"]
    lookup_columns = ["talkdesk__Case__c", "talkdesk__Talkdesk_Activity__c"]

    # Track statistics
    stats = {
        col: {"total": 0, "blank": 0, "non_blank": 0, "matched": 0, "unmatched": 0, "unique_values": set()}
        for col in user_columns + lookup_columns
    }

    # Track RFPD blanking stats
    rfpd_stats = {"total": 0, "blanked": 0}

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("🔄 Processing source file in chunks...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        detail_chunk = chunk.copy()

        # === RFPD BLANKING CHECK ===
        if "talkdesk__Case__c" in chunk.columns and "talkdesk__Case__r.recordtype.Name" in chunk.columns:
            recordtype_col = "talkdesk__Case__r.recordtype.Name"
            rfpd_mask = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD"
            rfpd_stats["total"] += len(chunk)
            rfpd_stats["blanked"] += rfpd_mask.sum()

            # Add RFPD flag column
            detail_chunk["talkdesk__Case__c_RFPD_Blank"] = rfpd_mask.map({True: "Y", False: "N"})

        # === USER COLUMNS AUDIT ===
        for col in user_columns:
            if col in chunk.columns:
                source_values = chunk[col].astype(str).str.strip()

                # Update stats
                stats[col]["total"] += len(chunk)
                stats[col]["blank"] += (source_values == "").sum()
                stats[col]["non_blank"] += (source_values != "").sum()
                stats[col]["unique_values"].update(source_values[source_values != ""].unique())

                # Create lookup column
                lkp_col = f"{col}_Lkp"
                detail_chunk[lkp_col] = source_values.apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )

                # Create flag column (Y = matched or will get default, N = source blank)
                flag_col = f"{col}_Flag"
                lkp_values = detail_chunk[lkp_col].astype(str).str.strip()

                # For standard user fields: Y if matched, Y if source had value (will get default)
                detail_chunk[flag_col] = "N"
                matched_mask = lkp_values != ""
                source_had_value = source_values != ""
                detail_chunk.loc[matched_mask | source_had_value, flag_col] = "Y"

                # Count matched
                stats[col]["matched"] += matched_mask.sum()
                stats[col]["unmatched"] += ((source_values != "") & (lkp_values == "")).sum()

        # === CASE LOOKUP AUDIT ===
        if "talkdesk__Case__c" in chunk.columns:
            col = "talkdesk__Case__c"
            source_values = chunk[col].astype(str).str.strip()

            # Check if RFPD blanking applies
            if "talkdesk__Case__r.recordtype.Name" in chunk.columns:
                rfpd_mask = chunk["talkdesk__Case__r.recordtype.Name"].astype(str).str.strip().str.upper() == "RFPD"
                # Values that will be blanked due to RFPD don't count as "source values" for lookup
                effective_source = source_values.copy()
                effective_source[rfpd_mask] = ""
            else:
                effective_source = source_values

            # Update stats (using effective source after RFPD blanking)
            stats[col]["total"] += len(chunk)
            stats[col]["blank"] += (effective_source == "").sum()
            stats[col]["non_blank"] += (effective_source != "").sum()
            stats[col]["unique_values"].update(effective_source[effective_source != ""].unique())

            # Create lookup column
            lkp_col = f"{col}_Lkp"
            detail_chunk[lkp_col] = effective_source.apply(
                lambda val: dict_case.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )

            # Create flag column
            flag_col = f"{col}_Flag"
            lkp_values = detail_chunk[lkp_col].astype(str).str.strip()
            detail_chunk[flag_col] = "N"
            matched_mask = lkp_values != ""
            detail_chunk.loc[matched_mask, flag_col] = "Y"

            # Count matched
            stats[col]["matched"] += matched_mask.sum()
            stats[col]["unmatched"] += ((effective_source != "") & (lkp_values == "")).sum()

        # === TALKDESK ACTIVITY LOOKUP AUDIT ===
        if "talkdesk__Talkdesk_Activity__c" in chunk.columns:
            col = "talkdesk__Talkdesk_Activity__c"
            source_values = chunk[col].astype(str).str.strip()

            # Update stats
            stats[col]["total"] += len(chunk)
            stats[col]["blank"] += (source_values == "").sum()
            stats[col]["non_blank"] += (source_values != "").sum()
            stats[col]["unique_values"].update(source_values[source_values != ""].unique())

            # Create lookup column
            lkp_col = f"{col}_Lkp"
            detail_chunk[lkp_col] = source_values.apply(
                lambda val: dict_talkdesk_activity.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )

            # Create flag column
            flag_col = f"{col}_Flag"
            lkp_values = detail_chunk[lkp_col].astype(str).str.strip()
            detail_chunk[flag_col] = "N"
            matched_mask = lkp_values != ""
            detail_chunk.loc[matched_mask, flag_col] = "Y"

            # Count matched
            stats[col]["matched"] += matched_mask.sum()
            stats[col]["unmatched"] += ((source_values != "") & (lkp_values == "")).sum()

        # Write detail report
        detail_chunk.to_csv(
            detail_report_file,
            index=False,
            mode="a" if header_written else "w",
            header=not header_written,
            encoding="utf-8-sig",
        )
        header_written = True
        total_rows += len(chunk)
        print(f"✅ Chunk {chunk_idx}: {len(chunk)} rows processed")

    # === GENERATE SUMMARY REPORT ===
    print("\n📝 Generating summary report...")

    summary_data = []

    # Add RFPD blanking summary
    summary_data.append({
        "Column": "talkdesk__Case__c (RFPD Blanking)",
        "Total Records": rfpd_stats["total"],
        "Blank": "-",
        "Non-Blank": "-",
        "Unique Values": "-",
        "Matched": "-",
        "Unmatched": "-",
        "Match Rate (%)": "-",
        "Will Be Blanked (RFPD)": rfpd_stats["blanked"],
    })

    # Add column statistics
    for col in user_columns + lookup_columns:
        s = stats[col]
        unique_count = len(s["unique_values"])
        match_rate = (s["matched"] / s["non_blank"] * 100) if s["non_blank"] > 0 else 0

        summary_data.append({
            "Column": col,
            "Total Records": s["total"],
            "Blank": s["blank"],
            "Non-Blank": s["non_blank"],
            "Unique Values": unique_count,
            "Matched": s["matched"],
            "Unmatched": s["unmatched"],
            "Match Rate (%)": f"{match_rate:.2f}",
            "Will Be Blanked (RFPD)": "-",
        })

    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(summary_report_file, index=False, encoding="utf-8-sig")

    # === PRINT SUMMARY ===
    print("\n" + "=" * 80)
    print("📊 AUDIT SUMMARY - TALKDESK ACTIVITY CASE RELATION")
    print("=" * 80)
    print(f"Total rows processed: {total_rows}")

    print("\n--- RFPD BLANKING ---")
    print(f"Total records: {rfpd_stats['total']}")
    print(f"Records to be blanked (RFPD): {rfpd_stats['blanked']}")

    print("\n--- STANDARD USER FIELDS ---")
    for col in user_columns:
        s = stats[col]
        print(f"\n{col}:")
        print(f"  Total: {s['total']} | Blank: {s['blank']} | Non-Blank: {s['non_blank']}")
        print(f"  Unique: {len(s['unique_values'])} | Matched: {s['matched']} | Unmatched: {s['unmatched']}")
        print(f"  Note: Blank/Unmatched will receive default: {DEFAULT_CREATEDBY_LASTMODIFIED_ID}")

    print("\n--- LOOKUP FIELDS ---")
    for col in lookup_columns:
        s = stats[col]
        match_rate = (s["matched"] / s["non_blank"] * 100) if s["non_blank"] > 0 else 0
        print(f"\n{col}:")
        print(f"  Total: {s['total']} | Blank: {s['blank']} | Non-Blank: {s['non_blank']}")
        print(f"  Unique: {len(s['unique_values'])} | Matched: {s['matched']} | Unmatched: {s['unmatched']}")
        print(f"  Match Rate: {match_rate:.2f}%")
        if s["unmatched"] > 0:
            print(f"  ⚠️ {s['unmatched']} records will have blank values (unmapped)")

    print("\n" + "=" * 80)
    print("📄 Output Files:")
    print(f"  Detail Report: {detail_report_file}")
    print(f"  Summary Report: {summary_report_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()

