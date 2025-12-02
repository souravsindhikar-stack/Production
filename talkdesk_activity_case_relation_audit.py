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

    print("=" * 80)
    print("TALKDESK ACTIVITY CASE RELATION - AUDIT SCRIPT")
    print("=" * 80)

    print("\nLoading lookup files...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"   User lookup: {len(user_lookup_dict)} mappings")
    
    dict_case = load_lookup_dict(CASE_LOOKUP_FILE)
    print(f"   Case lookup: {len(dict_case)} mappings")
    
    dict_talkdesk_activity = load_lookup_dict(TALKDESK_ACTIVITY_LOOKUP_FILE)
    print(f"   Talkdesk Activity lookup: {len(dict_talkdesk_activity)} mappings")

    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_DetailReport.csv")
    summary_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_SummaryReport.csv")

    # Remove existing output files
    for f in [detail_report_file, summary_report_file]:
        if os.path.exists(f):
            os.remove(f)

    # Stats for each column
    stats = {
        "CreatedById": {
            "total": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
            "default_applied": 0
        },
        "LastModifiedById": {
            "total": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
            "default_applied": 0
        },
        "talkdesk__Case__c": {
            "total": 0, "total_nonblank_original": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
            "blanked_by_recordtype": 0, "blanked_detail": {}
        },
        "talkdesk__Talkdesk_Activity__c": {
            "total": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
        },
    }

    # Find recordtype column (case-insensitive search)
    sample_df = pd.read_csv(SOURCE_FILE, dtype=str, nrows=5)
    recordtype_col = None
    
    # Look for recordtype column (case-insensitive)
    possible_names = [
        "talkdesk__Case__r.recordtype.Name",
        "talkdesk__Case__r.Recordtype.Name",
        "talkdesk__Case__r.RecordType.Name",
    ]
    for col_name in sample_df.columns:
        if col_name.lower() == "talkdesk__case__r.recordtype.name":
            recordtype_col = col_name
            break
    
    print(f"\n   Checking for recordtype column...")
    if recordtype_col:
        print(f"   ✅ Found recordtype column: {recordtype_col}")
        # Show sample values
        sample_vals = sample_df[recordtype_col].unique()[:5]
        print(f"   Sample values: {list(sample_vals)}")
    else:
        print(f"   ⚠️ Recordtype column NOT found!")
        print(f"   Looking for columns containing 'recordtype': ", end="")
        rt_cols = [c for c in sample_df.columns if "recordtype" in c.lower()]
        print(rt_cols if rt_cols else "None found")

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("\nProcessing source file...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        total_rows += len(chunk)

        # === STEP 1: IDENTIFY RFPD RECORDS FIRST (before any lookups) ===
        rfpd_mask = pd.Series([False] * len(chunk), index=chunk.index)
        if recordtype_col and recordtype_col in chunk.columns and "talkdesk__Case__c" in chunk.columns:
            recordtype_vals = chunk[recordtype_col].astype(str).str.strip().str.upper()
            rfpd_mask = recordtype_vals == "RFPD"
            
            # Count records that will be blanked (non-blank case values with RFPD recordtype)
            case_vals = chunk["talkdesk__Case__c"].astype(str).str.strip()
            blanked_count = ((case_vals != "") & rfpd_mask).sum()
            stats["talkdesk__Case__c"]["blanked_by_recordtype"] += blanked_count
            if "RFPD" not in stats["talkdesk__Case__c"]["blanked_detail"]:
                stats["talkdesk__Case__c"]["blanked_detail"]["RFPD"] = 0
            stats["talkdesk__Case__c"]["blanked_detail"]["RFPD"] += blanked_count

        # === USER COLUMNS (CreatedById, LastModifiedById) ===
        for col in ["CreatedById", "LastModifiedById"]:
            if col not in chunk.columns:
                continue

            source_vals = chunk[col].astype(str).str.strip()
            lkp_vals = source_vals.apply(lambda val: user_lookup_dict.get(val.lower(), "") if val else "")

            # Create flag column
            def get_flag(src, lkp):
                src = str(src).strip()
                lkp = str(lkp).strip()
                if not src:
                    return ""
                return "Y" if lkp else "N"

            flags = pd.Series([get_flag(s, l) for s, l in zip(source_vals, lkp_vals)])
            chunk[f"{col}_Lkp"] = lkp_vals
            chunk[f"{col}_Flag"] = flags

            # Stats
            for src, lkp in zip(source_vals, lkp_vals):
                stats[col]["total"] += 1
                if src:
                    stats[col]["total_nonblank"] += 1
                    stats[col]["unique_nonblank"].add(src)
                    if lkp:
                        stats[col]["matched"] += 1
                        stats[col]["unique_matched"].add(src)
                    else:
                        stats[col]["unmatched"] += 1
                        stats[col]["unique_unmatched"].add(src)
                        stats[col]["default_applied"] += 1

        # === CASE LOOKUP (with RFPD blanking) ===
        if "talkdesk__Case__c" in chunk.columns:
            col = "talkdesk__Case__c"
            source_vals = chunk[col].astype(str).str.strip()
            
            # Track original non-blank count (before RFPD blanking)
            original_nonblank_count = (source_vals != "").sum()
            stats[col]["total_nonblank_original"] += original_nonblank_count
            
            # Create effective source: blank out RFPD records
            effective_source = source_vals.copy()
            effective_source[rfpd_mask] = ""

            # Lookup on effective source (excluding RFPD records)
            lkp_vals = effective_source.apply(lambda val: dict_case.get(val.lower(), "") if val else "")

            # Create flag column - show BLANKED for RFPD records
            def get_flag(src, eff_src, lkp, is_rfpd):
                src = str(src).strip()
                eff_src = str(eff_src).strip()
                lkp = str(lkp).strip()
                if is_rfpd and src:
                    return "BLANKED"
                if not eff_src:
                    return ""
                return "Y" if lkp else "N"

            flags = pd.Series([get_flag(s, e, l, r) for s, e, l, r in zip(source_vals, effective_source, lkp_vals, rfpd_mask)])
            chunk[f"{col}_Lkp"] = lkp_vals
            chunk[f"{col}_Flag"] = flags

            # Add RFPD blanking flag column for visibility
            chunk[f"{col}_RFPD_Blanked"] = rfpd_mask.map({True: "Y", False: ""})

            # Stats (using effective source - RFPD records are excluded from matched/unmatched counts)
            stats[col]["total"] += len(chunk)
            for eff_src, lkp in zip(effective_source, lkp_vals):
                if eff_src:  # Only count non-RFPD records
                    stats[col]["total_nonblank"] += 1
                    stats[col]["unique_nonblank"].add(eff_src)
                    if lkp:
                        stats[col]["matched"] += 1
                        stats[col]["unique_matched"].add(eff_src)
                    else:
                        stats[col]["unmatched"] += 1
                        stats[col]["unique_unmatched"].add(eff_src)

        # === TALKDESK ACTIVITY LOOKUP ===
        if "talkdesk__Talkdesk_Activity__c" in chunk.columns:
            col = "talkdesk__Talkdesk_Activity__c"
            source_vals = chunk[col].astype(str).str.strip()
            lkp_vals = source_vals.apply(lambda val: dict_talkdesk_activity.get(val.lower(), "") if val else "")

            def get_flag(src, lkp):
                src = str(src).strip()
                lkp = str(lkp).strip()
                if not src:
                    return ""
                return "Y" if lkp else "N"

            flags = pd.Series([get_flag(s, l) for s, l in zip(source_vals, lkp_vals)])
            chunk[f"{col}_Lkp"] = lkp_vals
            chunk[f"{col}_Flag"] = flags

            # Stats
            for src, lkp in zip(source_vals, lkp_vals):
                stats[col]["total"] += 1
                if src:
                    stats[col]["total_nonblank"] += 1
                    stats[col]["unique_nonblank"].add(src)
                    if lkp:
                        stats[col]["matched"] += 1
                        stats[col]["unique_matched"].add(src)
                    else:
                        stats[col]["unmatched"] += 1
                        stats[col]["unique_unmatched"].add(src)

        chunk.to_csv(detail_report_file, index=False, mode="a" if header_written else "w", header=not header_written, encoding="utf-8-sig")
        header_written = True
        print(f"   Chunk {chunk_idx}: {len(chunk):,} rows processed")

    # Write summary
    print("\nWriting summary report...")

    summary_rows = []
    
    # User columns
    for col in ["CreatedById", "LastModifiedById"]:
        s = stats[col]
        row = {
            "Field": col,
            "Type": "user_standard",
            "Total_Records": s["total"],
            "Total_NonBlank": s["total_nonblank"],
            "Total_NonBlank_After_Blanking": "-",
            "Unique_NonBlank": len(s["unique_nonblank"]),
            "Matched": s["matched"],
            "Matched_Unique": len(s["unique_matched"]),
            "Unmatched": s["unmatched"],
            "Unmatched_Unique": len(s["unique_unmatched"]),
            "Blanked_By_RecordType": "-",
            "Blanked_RFPD": "-",
            "Default_Applied": s["default_applied"],
            "Default_Value": DEFAULT_CREATEDBY_LASTMODIFIED_ID,
        }
        summary_rows.append(row)

    # Case column - show both original and after-blanking counts
    s = stats["talkdesk__Case__c"]
    row = {
        "Field": "talkdesk__Case__c",
        "Type": "case",
        "Total_Records": s["total"],
        "Total_NonBlank": s["total_nonblank_original"],
        "Total_NonBlank_After_Blanking": s["total_nonblank"],
        "Unique_NonBlank": len(s["unique_nonblank"]),
        "Matched": s["matched"],
        "Matched_Unique": len(s["unique_matched"]),
        "Unmatched": s["unmatched"],
        "Unmatched_Unique": len(s["unique_unmatched"]),
        "Blanked_By_RecordType": s["blanked_by_recordtype"],
        "Blanked_RFPD": s["blanked_detail"].get("RFPD", 0),
        "Default_Applied": "-",
        "Default_Value": "-",
    }
    summary_rows.append(row)

    # Talkdesk Activity column
    s = stats["talkdesk__Talkdesk_Activity__c"]
    row = {
        "Field": "talkdesk__Talkdesk_Activity__c",
        "Type": "talkdesk_activity",
        "Total_Records": s["total"],
        "Total_NonBlank": s["total_nonblank"],
        "Total_NonBlank_After_Blanking": "-",
        "Unique_NonBlank": len(s["unique_nonblank"]),
        "Matched": s["matched"],
        "Matched_Unique": len(s["unique_matched"]),
        "Unmatched": s["unmatched"],
        "Unmatched_Unique": len(s["unique_unmatched"]),
        "Blanked_By_RecordType": "-",
        "Blanked_RFPD": "-",
        "Default_Applied": "-",
        "Default_Value": "-",
    }
    summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(summary_report_file, index=False, encoding="utf-8-sig")

    # Print summary
    print("\n" + "=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    print(f"\nTotal records in source: {total_rows:,}")

    print("\n--- USER LOOKUP FIELDS ---")
    for col in ["CreatedById", "LastModifiedById"]:
        s = stats[col]
        print(f"\n{col}:")
        print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
        print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
        print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
        print(f"   Default will be applied to: {s['default_applied']:,} records")
        print(f"   Default: {DEFAULT_CREATEDBY_LASTMODIFIED_ID}")

    print("\n--- CASE LOOKUP FIELD ---")
    s = stats["talkdesk__Case__c"]
    print(f"\ntalkdesk__Case__c:")
    print(f"   Total non-blank (original): {s['total_nonblank_original']:,}")
    print(f"   Total non-blank (after RFPD blanking): {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
    print(f"   Blanked by RecordType: {s['blanked_by_recordtype']:,}")
    for rt, cnt in s["blanked_detail"].items():
        print(f"      - {rt}: {cnt:,}")
    print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
    print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
    if s["unmatched"] > 0:
        print(f"   ⚠️ {s['unmatched']:,} records will have blank values (unmapped)")

    print("\n--- TALKDESK ACTIVITY LOOKUP FIELD ---")
    s = stats["talkdesk__Talkdesk_Activity__c"]
    print(f"\ntalkdesk__Talkdesk_Activity__c:")
    print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
    print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
    print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
    if s["unmatched"] > 0:
        print(f"   ⚠️ {s['unmatched']:,} records will have blank values (unmapped)")

    print("\n" + "=" * 80)
    print("AUDIT COMPLETED!")
    print("=" * 80)
    print(f"\nDetail Report: {detail_report_file}")
    print(f"Summary Report: {summary_report_file}")


if __name__ == "__main__":
    main()

