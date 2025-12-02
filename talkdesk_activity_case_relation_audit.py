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

    # Column configs
    columns_config = {
        "CreatedById": {"lookup": user_lookup_dict, "type": "user_standard", "default": DEFAULT_CREATEDBY_LASTMODIFIED_ID},
        "LastModifiedById": {"lookup": user_lookup_dict, "type": "user_standard", "default": DEFAULT_CREATEDBY_LASTMODIFIED_ID},
        "talkdesk__Case__c": {"lookup": dict_case, "type": "case", "recordtype_col": "talkdesk__Case__r.recordtype.Name", "blank_values": ["RFPD"]},
        "talkdesk__Talkdesk_Activity__c": {"lookup": dict_talkdesk_activity, "type": "talkdesk_activity"},
    }

    # Stats
    stats = {}
    for col in columns_config.keys():
        stats[col] = {
            "total": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "blanked_by_recordtype": 0, "blanked_detail": {},
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
            "default_applied": 0
        }

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("\nProcessing source file...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        total_rows += len(chunk)

        for col, config in columns_config.items():
            if col not in chunk.columns:
                continue

            lookup_dict = config["lookup"]
            col_type = config["type"]

            source_vals = chunk[col].astype(str).str.strip()
            
            # For case lookup, check RFPD blanking first
            effective_source = source_vals.copy()
            recordtype_col = config.get("recordtype_col")
            blank_values = config.get("blank_values", [])
            
            if recordtype_col and recordtype_col in chunk.columns and blank_values:
                recordtype_vals = chunk[recordtype_col].astype(str).str.strip().str.upper()
                
                for bv in blank_values:
                    mask = recordtype_vals == bv.upper()
                    count = ((source_vals != "") & mask).sum()  # Only count non-blank values being blanked
                    stats[col]["blanked_by_recordtype"] += count
                    if bv not in stats[col]["blanked_detail"]:
                        stats[col]["blanked_detail"][bv] = 0
                    stats[col]["blanked_detail"][bv] += count
                    # These values will be blanked, so don't count them as needing lookup
                    effective_source[mask] = ""

            lkp_vals = effective_source.apply(lambda val: lookup_dict.get(val.lower(), "") if val else "")

            def get_flag(src, lkp):
                src = str(src).strip()
                lkp = str(lkp).strip()
                if not src:
                    return ""
                return "Y" if lkp else "N"

            flags = pd.Series([get_flag(s, l) for s, l in zip(effective_source, lkp_vals)])

            chunk[f"{col}_Lkp"] = lkp_vals
            chunk[f"{col}_Flag"] = flags

            # Stats
            for src, eff_src, lkp in zip(source_vals, effective_source, lkp_vals):
                stats[col]["total"] += 1

                if eff_src:  # Use effective source (after RFPD blanking)
                    stats[col]["total_nonblank"] += 1
                    stats[col]["unique_nonblank"].add(eff_src)

                    if lkp:
                        stats[col]["matched"] += 1
                        stats[col]["unique_matched"].add(eff_src)
                    else:
                        stats[col]["unmatched"] += 1
                        stats[col]["unique_unmatched"].add(eff_src)

                        if col_type == "user_standard":
                            stats[col]["default_applied"] += 1

        chunk.to_csv(detail_report_file, index=False, mode="a" if header_written else "w", header=not header_written, encoding="utf-8-sig")
        header_written = True
        print(f"   Chunk {chunk_idx}: {len(chunk):,} rows processed")

    # Write summary
    print("\nWriting summary report...")

    summary_rows = []
    for col, config in columns_config.items():
        s = stats[col]
        col_type = config["type"]

        row = {
            "Field": col,
            "Type": col_type,
            "Total_Records": s["total"],
            "Total_NonBlank": s["total_nonblank"],
            "Unique_NonBlank": len(s["unique_nonblank"]),
            "Matched": s["matched"],
            "Matched_Unique": len(s["unique_matched"]),
            "Unmatched": s["unmatched"],
            "Unmatched_Unique": len(s["unique_unmatched"]),
            "Blanked_By_RecordType": s["blanked_by_recordtype"],
        }

        for rt, cnt in s["blanked_detail"].items():
            row[f"Blanked_{rt}"] = cnt

        if col_type == "user_standard":
            row["Default_Applied"] = s["default_applied"]
            row["Default_Value"] = config["default"]

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
        if col in stats:
            s = stats[col]
            cfg = columns_config[col]
            print(f"\n{col}:")
            print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
            print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
            print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
            print(f"   Default will be applied to: {s['default_applied']:,} records")
            print(f"   Default: {cfg['default']}")

    print("\n--- CASE LOOKUP FIELD ---")
    s = stats.get("talkdesk__Case__c", {})
    if s:
        print(f"\ntalkdesk__Case__c:")
        print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
        print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
        print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
        print(f"   Blanked by RecordType: {s['blanked_by_recordtype']:,}")
        for rt, cnt in s["blanked_detail"].items():
            print(f"      - {rt}: {cnt:,}")

    print("\n--- TALKDESK ACTIVITY LOOKUP FIELD ---")
    s = stats.get("talkdesk__Talkdesk_Activity__c", {})
    if s:
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

