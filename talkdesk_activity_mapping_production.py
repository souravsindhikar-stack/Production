import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Talkdesk Activity data
SOURCE_FILE = r"D:\Production\Source\TalkdeskActivity_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\Case_Lkp.csv"
ACCOUNT_LOOKUP_FILE = r"D:\Production\Lkp Files\Account_Lkp.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Contact_Lkp.csv"

# Contact verification files (for RFPD and Null Email check)
NULL_EMAIL_CONTACTS_FILE = r"D:\Production\Lkp Files\NullEmail_Contacts.csv"
RFPD_CONTACT_IDS_FILE = r"D:\Production\Lkp Files\RFPD_Contact_Ids.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
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


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
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
    
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    
    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df[key_col], df[value_col])
        if str(k).strip()
    }


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
    print("🚀 TALKDESK ACTIVITY - MAPPING")
    print("="*70)
    
    # === LOAD LOOKUP FILES ===
    print("\n📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Case lookup...")
    case_lookup_dict = load_simple_lookup(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("   • Account lookup...")
    account_lookup_dict = load_simple_lookup(ACCOUNT_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(account_lookup_dict)} account mappings")
    
    print("   • Contact lookup (18-char matching)...")
    contact_lookup_dict = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(contact_lookup_dict)} contact mappings")
    
    print("\n📖 Loading contact verification files...")
    print("   • RFPD contact IDs...")
    rfpd_contact_ids = load_id_set(RFPD_CONTACT_IDS_FILE, "Id")
    print(f"     ✅ Loaded {len(rfpd_contact_ids)} RFPD contact IDs")
    
    print("   • Null email contacts...")
    null_email_ids = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id")
    print(f"     ✅ Loaded {len(null_email_ids)} null email contact IDs")
    
    # === PREPARE OUTPUT FILES ===
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")
    
    if os.path.exists(main_output_file):
        os.remove(main_output_file)
    
    # === TRACK BLANKED RECORDS (by recordtype) ===
    blanked_data = {
        "Case_RFPD": [],
        "Case_Alliance": [],
        "Case_CXG": [],
        "Account_RFPD": [],
        "Contact_RFPD": [],
    }
    
    # === TRACK UNMAPPED RECORDS ===
    unmapped_data = {
        "talkdesk__User__c": [],
        "talkdesk__Case__c": [],
        "talkdesk__Account__c": [],
        "talkdesk__Contact__c": [],
        "talkdesk__Name_Id__c": [],
    }
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0
    
    print("\n" + "="*70)
    print("🔄 PROCESSING SOURCE FILE")
    print("="*70)
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        
        # Store original values for tracking
        original_values = {}
        for col in unmapped_data.keys():
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()
        
        # ================================================================
        # STEP 1: RECORD TYPE BLANKING (BEFORE MAPPING)
        # ================================================================
        
        # --- Case RecordType Blanking ---
        if "talkdesk__Case__c" in chunk.columns and "talkdesk__Case__r.recordtype.Name" in chunk.columns:
            recordtype_col = "talkdesk__Case__r.recordtype.Name"
            case_col = "talkdesk__Case__c"
            
            # RFPD
            mask_rfpd = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", case_col]].copy()
                blanked_data["Case_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, case_col] = ""
            
            # Alliance
            mask_alliance = chunk[recordtype_col].astype(str).str.strip().str.upper() == "ALLIANCE"
            if mask_alliance.any():
                blanked_rows = chunk.loc[mask_alliance, ["Id", case_col]].copy()
                blanked_data["Case_Alliance"].append(blanked_rows)
                chunk.loc[mask_alliance, case_col] = ""
            
            # CXG
            mask_cxg = chunk[recordtype_col].astype(str).str.strip().str.upper() == "CXG"
            if mask_cxg.any():
                blanked_rows = chunk.loc[mask_cxg, ["Id", case_col]].copy()
                blanked_data["Case_CXG"].append(blanked_rows)
                chunk.loc[mask_cxg, case_col] = ""
        
        # --- Account RecordType Blanking ---
        if "talkdesk__Account__c" in chunk.columns and "talkdesk__Account__r.Recordtype.Name" in chunk.columns:
            recordtype_col = "talkdesk__Account__r.Recordtype.Name"
            account_col = "talkdesk__Account__c"
            
            # RFPD Account
            mask_rfpd = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD ACCOUNT"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", account_col]].copy()
                blanked_data["Account_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, account_col] = ""
        
        # --- Contact RecordType Blanking ---
        if "talkdesk__Contact__c" in chunk.columns and "talkdesk__Contact__r.Account.Recordtype.Name" in chunk.columns:
            recordtype_col = "talkdesk__Contact__r.Account.Recordtype.Name"
            contact_col = "talkdesk__Contact__c"
            
            # RFPD Account
            mask_rfpd = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD ACCOUNT"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", contact_col]].copy()
                blanked_data["Contact_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, contact_col] = ""
        
        # Update original values after blanking for correct unmapped tracking
        for col in ["talkdesk__Case__c", "talkdesk__Account__c", "talkdesk__Contact__c"]:
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()
        
        # ================================================================
        # STEP 2: STANDARD MAPPING
        # ================================================================
        
        # --- Standard User Lookup (OwnerId, CreatedById, LastModifiedById) ---
        standard_user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]
        
        for col in standard_user_fields:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                if col == "OwnerId":
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_OWNER_ID
                else:
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID
        
        # --- talkdesk__User__c (blank stays blank, unmapped gets default) ---
        if "talkdesk__User__c" in chunk.columns:
            original = original_values.get("talkdesk__User__c", pd.Series([""] * len(chunk)))
            
            chunk["talkdesk__User__c"] = chunk["talkdesk__User__c"].apply(
                lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["talkdesk__User__c"].astype(str).str.strip()
            
            # Track unmapped (source had value but mapping failed)
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["talkdesk__User__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["talkdesk__User__c"].append(unmapped_rows[["Id", "talkdesk__User__c"]])
                
                # Apply default for unmapped
                chunk.loc[unmapped_mask, "talkdesk__User__c"] = DEFAULT_CREATEDBY_LASTMODIFIED_ID
        
        # --- Case Lookup ---
        if "talkdesk__Case__c" in chunk.columns:
            original = original_values.get("talkdesk__Case__c", pd.Series([""] * len(chunk)))
            
            chunk["talkdesk__Case__c"] = chunk["talkdesk__Case__c"].apply(
                lambda val: case_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["talkdesk__Case__c"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["talkdesk__Case__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["talkdesk__Case__c"].append(unmapped_rows[["Id", "talkdesk__Case__c"]])
        
        # --- Account Lookup ---
        if "talkdesk__Account__c" in chunk.columns:
            original = original_values.get("talkdesk__Account__c", pd.Series([""] * len(chunk)))
            
            chunk["talkdesk__Account__c"] = chunk["talkdesk__Account__c"].apply(
                lambda val: account_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["talkdesk__Account__c"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["talkdesk__Account__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["talkdesk__Account__c"].append(unmapped_rows[["Id", "talkdesk__Account__c"]])
        
        # --- Contact Lookups (18-char matching) ---
        contact_fields = ["talkdesk__Contact__c", "talkdesk__Name_Id__c"]
        
        for col in contact_fields:
            if col in chunk.columns:
                original = original_values.get(col, pd.Series([""] * len(chunk)))
                
                chunk[col] = chunk[col].apply(
                    lambda val: contact_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                mapped = chunk[col].astype(str).str.strip()
                
                unmapped_mask = (original != "") & (mapped == "")
                if unmapped_mask.any():
                    unmapped_rows = chunk[unmapped_mask].copy()
                    unmapped_rows[col] = original[unmapped_mask].values
                    if "Id" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Id", col]])
        
        # Write to main output
        chunk.to_csv(
            main_output_file,
            index=False,
            mode="a" if header_written else "w",
            header=not header_written,
            encoding="utf-8-sig",
        )
        header_written = True
        total_rows += len(chunk)
        
        print(f"   ✅ Chunk {chunk_idx}: {len(chunk):,} rows processed")
    
    # ================================================================
    # WRITE OUTPUT FILES
    # ================================================================
    
    print("\n" + "="*70)
    print("📝 WRITING OUTPUT FILES")
    print("="*70)
    
    # --- Write Blanked Files (by RecordType) ---
    print("\n📝 Blanked records (by RecordType):")
    
    blanked_file_names = {
        "Case_RFPD": "talkdesk__Case__c_Blanked_RFPD.csv",
        "Case_Alliance": "talkdesk__Case__c_Blanked_Alliance.csv",
        "Case_CXG": "talkdesk__Case__c_Blanked_CXG.csv",
        "Account_RFPD": "talkdesk__Account__c_Blanked_RFPD.csv",
        "Contact_RFPD": "talkdesk__Contact__c_Blanked_RFPD.csv",
    }
    
    blanked_counts = {}
    for key, data_list in blanked_data.items():
        if data_list:
            blanked_df = pd.concat(data_list, ignore_index=True)
            blanked_file = os.path.join(OUTPUT_DIR, blanked_file_names[key])
            blanked_df.to_csv(blanked_file, index=False, encoding="utf-8-sig")
            blanked_counts[key] = len(blanked_df)
            print(f"   ✅ {key}: {len(blanked_df):,} records → {blanked_file}")
        else:
            blanked_counts[key] = 0
    
    # --- Write Unmapped Files ---
    print("\n📝 Unmapped records:")
    
    unmapped_counts = {}
    for col, data_list in unmapped_data.items():
        if data_list:
            unmapped_df = pd.concat(data_list, ignore_index=True)
            unmapped_file = os.path.join(OUTPUT_DIR, f"{col}_unmapped.csv")
            unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
            unmapped_counts[col] = len(unmapped_df)
            print(f"   ⚠️ {col}: {len(unmapped_df):,} unmapped → {unmapped_file}")
        else:
            unmapped_counts[col] = 0
    
    # --- Write Contact Verification Files (RFPD + NullEmail check) ---
    print("\n📝 Contact verification files:")
    
    for col in ["talkdesk__Contact__c", "talkdesk__Name_Id__c"]:
        if unmapped_data[col]:
            unmapped_df = pd.concat(unmapped_data[col], ignore_index=True)
            contact_ids = unmapped_df[col].astype(str).str.strip()
            
            unmapped_df["In_RFPD"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).lower() in rfpd_contact_ids else "FALSE"
            )
            unmapped_df["In_nullemail"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).lower() in null_email_ids else "FALSE"
            )
            
            verification_file = os.path.join(OUTPUT_DIR, f"{col}_verification.csv")
            unmapped_df.to_csv(verification_file, index=False, encoding="utf-8-sig")
            
            in_rfpd_count = (unmapped_df["In_RFPD"] == "TRUE").sum()
            in_nullemail_count = (unmapped_df["In_nullemail"] == "TRUE").sum()
            
            print(f"   ✅ {col} verification → {verification_file}")
            print(f"      • In RFPD: {in_rfpd_count:,}")
            print(f"      • In Null Email: {in_nullemail_count:,}")
    
    # ================================================================
    # FINAL SUMMARY
    # ================================================================
    
    print("\n" + "="*70)
    print("✅ TALKDESK ACTIVITY MAPPING COMPLETED!")
    print("="*70)
    print(f"\n📊 Total rows processed: {total_rows:,}")
    print(f"📝 Main output: {main_output_file}")
    
    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")
    
    # Blanked summary
    total_blanked = sum(blanked_counts.values())
    if total_blanked > 0:
        print(f"\n📋 BLANKED BY RECORDTYPE: {total_blanked:,} total")
        for key, count in blanked_counts.items():
            if count > 0:
                print(f"   - {key}: {count:,}")
    
    # Unmapped summary
    total_unmapped = sum(unmapped_counts.values())
    if total_unmapped > 0:
        print(f"\n⚠️ UNMAPPED: {total_unmapped:,} total")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count:,}")


if __name__ == "__main__":
    main()
