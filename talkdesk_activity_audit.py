import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Talkdesk Activity data
SOURCE_FILE = r"D:\Production\Talkdesk_Activity\Talkdesk_Activity_v1.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\All_Case_Destination_Org_v1_3035227.csv"
ACCOUNT_LOOKUP_FILE = r"D:\Production\Lkp Files\Account_legacyId_From DestinationOrg.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Prod_Ecom_Contact_Lkp_File.csv"

# Contact verification files
NULL_EMAIL_CONTACTS_FILE = r"D:\Production\Lkp Files\Contact_Null_Email_DigitalProd.csv"
RFPD_CONTACT_IDS_FILE = r"D:\Production\Lkp Files\RFPD_Contact_DigitalSourceOrg.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output\Audit"

# ========= CONSTANTS =========
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# Account constants by RecordType
ACCOUNT_UNITY_ID = "001Vq00000bXYaIIAW"
ACCOUNT_ARROW_VERTICAL_ID = "001Vq00000bXUGZIA4"

CHUNK_SIZE = 50_000


def load_user_lookup(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"User lookup file not found: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    df["Legacy_SF_Record_ID__c"] = df["Legacy_SF_Record_ID__c"].astype(str).str.strip()
    df["Id"] = df["Id"].astype(str).str.strip()
    return {str(k).strip().lower(): str(v).strip() for k, v in zip(df["Legacy_SF_Record_ID__c"], df["Id"]) if str(k).strip()}


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lookup file not found: {path}")
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    df = df.fillna("")
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    return {str(k).strip().lower(): str(v).strip() for k, v in zip(df[key_col], df[value_col]) if str(k).strip()}


def load_id_set(path, id_col="Id"):
    if not os.path.exists(path):
        print(f"   Warning: File not found: {path}")
        return set()
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    df = df.fillna("")
    if id_col not in df.columns:
        print(f"   Warning: Column '{id_col}' not found in {path}")
        return set()
    return {str(v).strip().lower() for v in df[id_col] if str(v).strip()}


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    print("=" * 80)
    print("TALKDESK ACTIVITY - AUDIT SCRIPT")
    print("=" * 80)
    
    # Load lookups
    print("\nLoading lookup files...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"   User lookup: {len(user_lookup_dict)} mappings")
    
    case_lookup_dict = load_simple_lookup(CASE_LOOKUP_FILE)
    print(f"   Case lookup: {len(case_lookup_dict)} mappings")
    
    account_lookup_dict = load_simple_lookup(ACCOUNT_LOOKUP_FILE)
    print(f"   Account lookup: {len(account_lookup_dict)} mappings")
    
    contact_lookup_dict = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"   Contact lookup: {len(contact_lookup_dict)} mappings")
    
    print("\nLoading contact verification files...")
    rfpd_contact_ids = load_id_set(RFPD_CONTACT_IDS_FILE, "Id")
    print(f"   RFPD contact IDs: {len(rfpd_contact_ids)}")
    
    null_email_ids = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id")
    print(f"   Null email contact IDs: {len(null_email_ids)}")
    
    # Column configs
    columns_config = {
        "OwnerId": {"lookup": user_lookup_dict, "type": "user_standard", "default": DEFAULT_OWNER_ID},
        "CreatedById": {"lookup": user_lookup_dict, "type": "user_standard", "default": DEFAULT_CREATEDBY_LASTMODIFIED_ID},
        "LastModifiedById": {"lookup": user_lookup_dict, "type": "user_standard", "default": DEFAULT_CREATEDBY_LASTMODIFIED_ID},
        "talkdesk__User__c": {"lookup": user_lookup_dict, "type": "user_custom", "default": DEFAULT_CREATEDBY_LASTMODIFIED_ID},
        "talkdesk__Case__c": {"lookup": case_lookup_dict, "type": "case", "recordtype_col": "talkdesk__Case__r.recordtype.Name", "blank_values": ["RFPD", "ALLIANCE", "CXG"]},
        "talkdesk__Account__c": {"lookup": account_lookup_dict, "type": "account", "recordtype_col": "talkdesk__Account__r.Recordtype.Name", "blank_values": ["RFPD ACCOUNT"], "constant_values": {"UNITY": ACCOUNT_UNITY_ID, "ARROW / VERICAL": ACCOUNT_ARROW_VERTICAL_ID}},
        "talkdesk__Contact__c": {"lookup": contact_lookup_dict, "type": "contact", "recordtype_col": "talkdesk__Contact__r.Account.Recordtype.Name", "blank_values": ["RFPD ACCOUNT"]},
        "talkdesk__Name_Id__c": {"lookup": contact_lookup_dict, "type": "contact", "recordtype_col": None, "blank_values": []},
    }
    
    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_DetailReport.csv")
    summary_report_file = os.path.join(OUTPUT_DIR, f"{source_basename}_SummaryReport.csv")
    
    if os.path.exists(detail_report_file):
        os.remove(detail_report_file)
    
    # Stats
    stats = {}
    for col in columns_config.keys():
        stats[col] = {
            "total": 0, "total_nonblank": 0, "matched": 0, "unmatched": 0,
            "blanked_by_recordtype": 0, "blanked_detail": {},
            "constant_by_recordtype": 0, "constant_detail": {},
            "unique_nonblank": set(), "unique_matched": set(), "unique_unmatched": set(),
            "unmatched_in_rfpd": set(), "unmatched_in_nullemail": set(), "unmatched_in_neither": set(),
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
            lkp_vals = source_vals.apply(lambda val: lookup_dict.get(val.lower(), "") if val else "")
            
            def get_flag(src, lkp):
                src = str(src).strip()
                lkp = str(lkp).strip()
                if not src:
                    return ""
                return "Y" if lkp else "N"
            
            flags = pd.Series([get_flag(s, l) for s, l in zip(source_vals, lkp_vals)])
            
            chunk[f"{col}_Lkp"] = lkp_vals
            chunk[f"{col}_Flag"] = flags
            
            # Recordtype blanking check
            recordtype_col = config.get("recordtype_col")
            blank_values = config.get("blank_values", [])
            constant_values = config.get("constant_values", {})
            
            if recordtype_col and recordtype_col in chunk.columns:
                recordtype_vals = chunk[recordtype_col].astype(str).str.strip().str.upper()
                
                # Track blanking by recordtype
                for bv in blank_values:
                    mask = recordtype_vals == bv.upper()
                    count = mask.sum()
                    stats[col]["blanked_by_recordtype"] += count
                    if bv not in stats[col]["blanked_detail"]:
                        stats[col]["blanked_detail"][bv] = 0
                    stats[col]["blanked_detail"][bv] += count
                
                # Track constant value assignments by recordtype
                for cv_name, cv_value in constant_values.items():
                    mask = recordtype_vals == cv_name.upper()
                    count = mask.sum()
                    stats[col]["constant_by_recordtype"] += count
                    if cv_name not in stats[col]["constant_detail"]:
                        stats[col]["constant_detail"][cv_name] = {"count": 0, "value": cv_value}
                    stats[col]["constant_detail"][cv_name]["count"] += count
            
            # Stats
            for src, lkp, flag in zip(source_vals, lkp_vals, flags):
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
                        
                        if col_type == "contact":
                            src_lower = src.lower()
                            if src_lower in rfpd_contact_ids:
                                stats[col]["unmatched_in_rfpd"].add(src)
                            elif src_lower in null_email_ids:
                                stats[col]["unmatched_in_nullemail"].add(src)
                            else:
                                stats[col]["unmatched_in_neither"].add(src)
                        
                        if col_type in ["user_standard", "user_custom"]:
                            if col_type == "user_standard" or (col_type == "user_custom" and src):
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
            "Constant_By_RecordType": s["constant_by_recordtype"],
        }
        
        for rt, cnt in s["blanked_detail"].items():
            row[f"Blanked_{rt}"] = cnt
        
        for rt, info in s["constant_detail"].items():
            row[f"Constant_{rt}"] = info["count"]
            row[f"Constant_{rt}_Value"] = info["value"]
        
        if col_type == "contact":
            row["Unmatched_In_RFPD"] = len(s["unmatched_in_rfpd"])
            row["Unmatched_In_NullEmail"] = len(s["unmatched_in_nullemail"])
            row["Unmatched_In_Neither"] = len(s["unmatched_in_neither"])
        
        if col_type in ["user_standard", "user_custom"]:
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
    for col in ["OwnerId", "CreatedById", "LastModifiedById", "talkdesk__User__c"]:
        if col in stats:
            s = stats[col]
            cfg = columns_config[col]
            print(f"\n{col}:")
            print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
            print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
            print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
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
    
    print("\n--- ACCOUNT LOOKUP FIELD ---")
    s = stats.get("talkdesk__Account__c", {})
    if s:
        print(f"\ntalkdesk__Account__c:")
        print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
        print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
        print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
        print(f"   Blanked by RecordType: {s['blanked_by_recordtype']:,}")
        for rt, cnt in s["blanked_detail"].items():
            print(f"      - {rt}: {cnt:,}")
        print(f"   Constant by RecordType: {s['constant_by_recordtype']:,}")
        for rt, info in s["constant_detail"].items():
            print(f"      - {rt}: {info['count']:,} -> {info['value']}")
    
    print("\n--- CONTACT LOOKUP FIELDS ---")
    for col in ["talkdesk__Contact__c", "talkdesk__Name_Id__c"]:
        s = stats.get(col, {})
        if s:
            print(f"\n{col}:")
            print(f"   Total non-blank: {s['total_nonblank']:,}, Unique: {len(s['unique_nonblank']):,}")
            print(f"   Matched: {s['matched']:,} (unique: {len(s['unique_matched']):,})")
            print(f"   Unmatched: {s['unmatched']:,} (unique: {len(s['unique_unmatched']):,})")
            if col == "talkdesk__Contact__c":
                print(f"   Blanked by RecordType: {s['blanked_by_recordtype']:,}")
                for rt, cnt in s["blanked_detail"].items():
                    print(f"      - {rt}: {cnt:,}")
            print(f"   Unmatched breakdown:")
            print(f"      - In RFPD: {len(s['unmatched_in_rfpd']):,}")
            print(f"      - In Null Email: {len(s['unmatched_in_nullemail']):,}")
            print(f"      - In Neither: {len(s['unmatched_in_neither']):,}")
    
    print("\n" + "=" * 80)
    print("AUDIT COMPLETED!")
    print("=" * 80)
    print(f"\nDetail Report: {detail_report_file}")
    print(f"Summary Report: {summary_report_file}")


if __name__ == "__main__":
    main()

