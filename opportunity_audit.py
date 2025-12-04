import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Opportunity data
SOURCE_FILE = r"D:\Production\Opportunity\Opportunity_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
ACCOUNT_LOOKUP_FILE = r"D:\Production\Lkp Files\Account_legacyId_From DestinationOrg.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Prod_Ecom_Contact_Lkp_File.csv"
CAMPAIGN_LOOKUP_FILE = r"D:\Production\Lkp Files\Campaign_Lkp_File.csv"

# RecordTypeId lookup file (Excel with Source and Destination sheets)
RECORDTYPEID_LOOKUP_FILE = r"D:\Production\Lkp Files\RecordTypeId_Lookup.xlsx"

# Contact verification files (for RFPD and Null Email check)
NULL_EMAIL_CONTACTS_FILE = r"D:\Production\Lkp Files\Contact_Null_Email_DigitalProd.csv"
RFPD_CONTACT_IDS_FILE = r"D:\Production\Lkp Files\RFPD_Contact_DigitalSourceOrg.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_lookup_dict(path, key_col, value_col):
    """Load a lookup file and return dictionary for mapping"""
    if not os.path.exists(path):
        print(f"   ⚠️ File not found: {path}")
        return {}
    
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    
    df = df.fillna("")
    
    if key_col not in df.columns or value_col not in df.columns:
        print(f"   ⚠️ Columns '{key_col}' or '{value_col}' not found in {path}")
        return {}
    
    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df[key_col], df[value_col])
        if str(k).strip()
    }


def load_recordtypeid_lookup(path):
    """
    Load RecordTypeId lookup from Excel file with two sheets: Source and Destination
    Returns a dictionary that maps source RecordTypeId to destination RecordTypeId
    """
    if not os.path.exists(path):
        print(f"   ⚠️ RecordTypeId lookup file not found: {path}")
        return {}
    
    # Read Source sheet: Id -> DeveloperName
    source_df = pd.read_excel(path, sheet_name="Source", dtype=str).fillna("")
    source_df["Id"] = source_df["Id"].astype(str).str.strip()
    source_df["DeveloperName"] = source_df["DeveloperName"].astype(str).str.strip()
    
    source_to_devname = {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(source_df["Id"], source_df["DeveloperName"])
        if str(k).strip()
    }
    
    # Read Destination sheet: DeveloperName -> Id
    dest_df = pd.read_excel(path, sheet_name="Destination", dtype=str).fillna("")
    dest_df["DeveloperName"] = dest_df["DeveloperName"].astype(str).str.strip()
    dest_df["Id"] = dest_df["Id"].astype(str).str.strip()
    
    devname_to_dest = {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(dest_df["DeveloperName"], dest_df["Id"])
        if str(k).strip()
    }
    
    # Build direct mapping: source Id -> destination Id
    result_dict = {}
    for source_id, dev_name in source_to_devname.items():
        if dev_name:
            dest_id = devname_to_dest.get(dev_name.lower(), "")
            if dest_id:
                result_dict[source_id] = dest_id
    
    return result_dict


def load_id_set(path, id_col="Id"):
    """Load a file and return set of IDs for checking"""
    if not os.path.exists(path):
        print(f"   ⚠️ File not found: {path}")
        return set()
    
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    
    df = df.fillna("")
    
    if id_col not in df.columns:
        print(f"   ⚠️ Column '{id_col}' not found in {path}")
        return set()
    
    return {str(v).strip().lower() for v in df[id_col] if str(v).strip()}


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    print("="*70)
    print("🔍 OPPORTUNITY - AUDIT REPORT")
    print("="*70)
    
    # === LOAD LOOKUP FILES ===
    print("\n📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_lookup_dict(USER_LOOKUP_FILE, "Legacy_SF_Record_ID__c", "Id")
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Account lookup...")
    account_lookup_dict = load_lookup_dict(ACCOUNT_LOOKUP_FILE, "Legacy_SF_Record_ID__c", "Id")
    print(f"     ✅ Loaded {len(account_lookup_dict)} account mappings")
    
    print("   • Contact lookup...")
    contact_lookup_dict = load_lookup_dict(CONTACT_LOOKUP_FILE, "Legacy_SF_Record_ID__c", "Id")
    print(f"     ✅ Loaded {len(contact_lookup_dict)} contact mappings")
    
    print("   • Campaign lookup...")
    campaign_lookup_dict = load_lookup_dict(CAMPAIGN_LOOKUP_FILE, "Legacy_SF_Record_ID__c", "Id")
    print(f"     ✅ Loaded {len(campaign_lookup_dict)} campaign mappings")
    
    print("   • RecordTypeId lookup...")
    recordtypeid_lookup_dict = load_recordtypeid_lookup(RECORDTYPEID_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(recordtypeid_lookup_dict)} RecordTypeId mappings")
    
    print("\n📖 Loading contact verification files...")
    print("   • RFPD contact IDs...")
    rfpd_contact_ids = load_id_set(RFPD_CONTACT_IDS_FILE, "Id")
    print(f"     ✅ Loaded {len(rfpd_contact_ids)} RFPD contact IDs")
    
    print("   • Null email contacts...")
    null_email_ids = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id")
    print(f"     ✅ Loaded {len(null_email_ids)} null email contact IDs")
    
    # === DEFINE AUDIT COLUMNS ===
    audit_columns = {
        # User lookups
        "OwnerId": {"type": "user", "lookup_dict": user_lookup_dict},
        "CreatedById": {"type": "user", "lookup_dict": user_lookup_dict},
        "LastModifiedById": {"type": "user", "lookup_dict": user_lookup_dict},
        # Account lookup
        "AccountId": {
            "type": "account", 
            "lookup_dict": account_lookup_dict,
            "recordtype_col": "Account.recordtype.Name"
        },
        # Contact lookups
        "Primary_Contact__c": {
            "type": "contact", 
            "lookup_dict": contact_lookup_dict,
            "recordtype_col": "Primary_Contact__r.Account.recordtype.Name"
        },
        "Purchasing_Contact__c": {
            "type": "contact", 
            "lookup_dict": contact_lookup_dict,
            "recordtype_col": "Purchasing_Contact__r.Account.recordtype.Name"
        },
        "ContactId": {"type": "contact", "lookup_dict": contact_lookup_dict},
        # Campaign lookup
        "CampaignId": {"type": "campaign", "lookup_dict": campaign_lookup_dict},
        # RecordTypeId lookup
        "RecordTypeId": {"type": "recordtype", "lookup_dict": recordtypeid_lookup_dict},
    }
    
    # === PREPARE ACCUMULATORS ===
    detail_records = []
    summary_data = {}
    
    for col in audit_columns:
        summary_data[col] = {
            "total": 0,
            "total_unique": set(),
            "blank": 0,
            "matched": 0,
            "matched_unique": set(),
            "unmatched": 0,
            "unmatched_unique": set(),
            "blanked_by_recordtype": 0,
            "in_rfpd": 0,
            "in_nullemail": 0,
        }
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    total_rows = 0
    
    print("\n" + "="*70)
    print("🔄 PROCESSING SOURCE FILE")
    print("="*70)
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        chunk_len = len(chunk)
        total_rows += chunk_len
        
        # Build detail record for each row
        for idx, row in chunk.iterrows():
            detail_row = {"Id": row.get("Id", "")}
            
            for col, config in audit_columns.items():
                if col not in chunk.columns:
                    continue
                
                source_val = str(row[col]).strip()
                lookup_dict = config["lookup_dict"]
                
                # Check recordtype blanking first
                recordtype_col = config.get("recordtype_col")
                blanked_by_rt = False
                
                if recordtype_col and recordtype_col in chunk.columns:
                    rt_val = str(row[recordtype_col]).strip().upper()
                    if rt_val == "RFPD ACCOUNT":
                        blanked_by_rt = True
                
                # Lookup
                if blanked_by_rt:
                    lkp_val = ""
                    flag = "BLANKED"
                    summary_data[col]["blanked_by_recordtype"] += 1
                elif not source_val:
                    lkp_val = ""
                    flag = ""
                    summary_data[col]["blank"] += 1
                else:
                    lkp_val = lookup_dict.get(source_val.lower(), "")
                    if lkp_val:
                        flag = "Y"
                        summary_data[col]["matched"] += 1
                        summary_data[col]["matched_unique"].add(source_val.lower())
                    else:
                        flag = "N"
                        summary_data[col]["unmatched"] += 1
                        summary_data[col]["unmatched_unique"].add(source_val.lower())
                        
                        # Check RFPD and NullEmail for contact columns
                        if config["type"] == "contact":
                            if source_val.lower() in rfpd_contact_ids:
                                summary_data[col]["in_rfpd"] += 1
                            if source_val.lower() in null_email_ids:
                                summary_data[col]["in_nullemail"] += 1
                
                summary_data[col]["total"] += 1
                if source_val:
                    summary_data[col]["total_unique"].add(source_val.lower())
                
                detail_row[col] = source_val
                detail_row[f"{col}_Lkp"] = lkp_val
                detail_row[f"{col}_Flag"] = flag
            
            detail_records.append(detail_row)
        
        print(f"   ✅ Chunk {chunk_idx}: {chunk_len:,} rows processed")
    
    # ================================================================
    # WRITE OUTPUT FILES
    # ================================================================
    
    print("\n" + "="*70)
    print("📝 WRITING AUDIT OUTPUT FILES")
    print("="*70)
    
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    
    # --- Write Detail Report ---
    detail_df = pd.DataFrame(detail_records)
    detail_file = os.path.join(OUTPUT_DIR, f"{source_basename}_Audit_DetailReport.csv")
    detail_df.to_csv(detail_file, index=False, encoding="utf-8-sig")
    print(f"\n📋 Detail Report: {detail_file}")
    
    # --- Write Summary Report ---
    summary_records = []
    
    for col, stats in summary_data.items():
        if col not in audit_columns:
            continue
        
        config = audit_columns[col]
        
        record = {
            "Column": col,
            "Type": config["type"],
            "Total_Records": stats["total"],
            "Total_Unique": len(stats["total_unique"]),
            "Blank_In_Source": stats["blank"],
            "Matched": stats["matched"],
            "Matched_Unique": len(stats["matched_unique"]),
            "Unmatched": stats["unmatched"],
            "Unmatched_Unique": len(stats["unmatched_unique"]),
        }
        
        # Add recordtype blanking count if applicable
        if "recordtype_col" in config:
            record["Blanked_By_RFPD_RecordType"] = stats["blanked_by_recordtype"]
        else:
            record["Blanked_By_RFPD_RecordType"] = ""
        
        # Add RFPD/NullEmail counts for contact columns
        if config["type"] == "contact":
            record["Unmatched_In_RFPD"] = stats["in_rfpd"]
            record["Unmatched_In_NullEmail"] = stats["in_nullemail"]
        else:
            record["Unmatched_In_RFPD"] = ""
            record["Unmatched_In_NullEmail"] = ""
        
        summary_records.append(record)
    
    summary_df = pd.DataFrame(summary_records)
    summary_file = os.path.join(OUTPUT_DIR, f"{source_basename}_Audit_SummaryReport.csv")
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    print(f"📊 Summary Report: {summary_file}")
    
    # ================================================================
    # CONSOLE SUMMARY
    # ================================================================
    
    print("\n" + "="*70)
    print("📊 AUDIT SUMMARY")
    print("="*70)
    
    print(f"\n📋 Total Rows in Source: {total_rows:,}")
    
    # --- User Lookups ---
    print("\n--- USER LOOKUPS ---")
    for col in ["OwnerId", "CreatedById", "LastModifiedById"]:
        if col in summary_data:
            stats = summary_data[col]
            print(f"\n{col}:")
            print(f"   Total: {stats['total']:,} | Unique: {len(stats['total_unique']):,}")
            print(f"   Blank: {stats['blank']:,}")
            print(f"   Matched: {stats['matched']:,} | Unique: {len(stats['matched_unique']):,}")
            print(f"   Unmatched: {stats['unmatched']:,} | Unique: {len(stats['unmatched_unique']):,}")
    
    # --- Account Lookup ---
    print("\n--- ACCOUNT LOOKUP ---")
    if "AccountId" in summary_data:
        stats = summary_data["AccountId"]
        print(f"\nAccountId:")
        print(f"   Total: {stats['total']:,} | Unique: {len(stats['total_unique']):,}")
        print(f"   Blank: {stats['blank']:,}")
        print(f"   Blanked by RFPD RecordType: {stats['blanked_by_recordtype']:,}")
        print(f"   Matched: {stats['matched']:,} | Unique: {len(stats['matched_unique']):,}")
        print(f"   Unmatched: {stats['unmatched']:,} | Unique: {len(stats['unmatched_unique']):,}")
    
    # --- Contact Lookups ---
    print("\n--- CONTACT LOOKUPS ---")
    for col in ["Primary_Contact__c", "Purchasing_Contact__c", "ContactId"]:
        if col in summary_data:
            stats = summary_data[col]
            print(f"\n{col}:")
            print(f"   Total: {stats['total']:,} | Unique: {len(stats['total_unique']):,}")
            print(f"   Blank: {stats['blank']:,}")
            if stats["blanked_by_recordtype"] > 0:
                print(f"   Blanked by RFPD RecordType: {stats['blanked_by_recordtype']:,}")
            print(f"   Matched: {stats['matched']:,} | Unique: {len(stats['matched_unique']):,}")
            print(f"   Unmatched: {stats['unmatched']:,} | Unique: {len(stats['unmatched_unique']):,}")
            if stats['in_rfpd'] > 0 or stats['in_nullemail'] > 0:
                print(f"   Unmatched in RFPD: {stats['in_rfpd']:,}")
                print(f"   Unmatched in NullEmail: {stats['in_nullemail']:,}")
    
    # --- Campaign Lookup ---
    print("\n--- CAMPAIGN LOOKUP ---")
    if "CampaignId" in summary_data:
        stats = summary_data["CampaignId"]
        print(f"\nCampaignId:")
        print(f"   Total: {stats['total']:,} | Unique: {len(stats['total_unique']):,}")
        print(f"   Blank: {stats['blank']:,}")
        print(f"   Matched: {stats['matched']:,} | Unique: {len(stats['matched_unique']):,}")
        print(f"   Unmatched: {stats['unmatched']:,} | Unique: {len(stats['unmatched_unique']):,}")
    
    # --- RecordTypeId Lookup ---
    print("\n--- RECORDTYPEID LOOKUP ---")
    if "RecordTypeId" in summary_data:
        stats = summary_data["RecordTypeId"]
        print(f"\nRecordTypeId:")
        print(f"   Total: {stats['total']:,} | Unique: {len(stats['total_unique']):,}")
        print(f"   Blank: {stats['blank']:,}")
        print(f"   Matched: {stats['matched']:,} | Unique: {len(stats['matched_unique']):,}")
        print(f"   Unmatched: {stats['unmatched']:,} | Unique: {len(stats['unmatched_unique']):,}")
    
    print("\n" + "="*70)
    print("✅ AUDIT COMPLETED!")
    print("="*70)


if __name__ == "__main__":
    main()

