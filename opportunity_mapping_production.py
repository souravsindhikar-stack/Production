import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Opportunity data
SOURCE_FILE = r"D:\Production\Opportunity\Opportunity_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
ACCOUNT_LOOKUP_FILE = r"D:\Production\Lkp Files\Account_legacyId_From DestinationOrg.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Prod_Ecom_Contact_Lkp_File.csv"

# RecordTypeId lookup file (Excel with Source and Destination sheets)
RECORDTYPEID_LOOKUP_FILE = r"D:\Production\Lkp Files\RecordTypeId_Lookup.xlsx"

# Contact verification files (for RFPD and Null Email check)
NULL_EMAIL_CONTACTS_FILE = r"D:\Production\Lkp Files\Contact_Null_Email_DigitalProd.csv"
RFPD_CONTACT_IDS_FILE = r"D:\Production\Lkp Files\RFPD_Contact_DigitalSourceOrg.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# Account constants by RecordType
ACCOUNT_UNITY_ID = "001Vq00000bXYaIIAW"
ACCOUNT_ARROW_VERTICAL_ID = "001Vq00000bXUGZIA4"

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


def load_recordtypeid_lookup(path):
    """
    Load RecordTypeId lookup from Excel file with two sheets: Source and Destination
    Returns a function that maps source RecordTypeId to destination RecordTypeId
    via DeveloperName bridge
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"RecordTypeId lookup file not found: {path}")
    
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
    
    print(f"     • Source sheet: {len(source_to_devname)} RecordType mappings")
    print(f"     • Destination sheet: {len(devname_to_dest)} RecordType mappings")
    
    def map_recordtypeid(source_id):
        """Map source RecordTypeId to destination RecordTypeId via DeveloperName"""
        source_id_clean = str(source_id).strip()
        if not source_id_clean:
            return ""
        
        # Step 1: Get DeveloperName from source Id
        dev_name = source_to_devname.get(source_id_clean.lower(), "")
        if not dev_name:
            return None  # Will be tracked as unmapped
        
        # Step 2: Get destination Id from DeveloperName
        dest_id = devname_to_dest.get(dev_name.lower(), "")
        if not dest_id:
            return None  # Will be tracked as unmapped
        
        return dest_id
    
    return map_recordtypeid


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
    print("🚀 OPPORTUNITY - PRODUCTION MAPPING")
    print("="*70)
    
    # === LOAD LOOKUP FILES ===
    print("\n📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Account lookup...")
    account_lookup_dict = load_simple_lookup(ACCOUNT_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(account_lookup_dict)} account mappings")
    
    print("   • Contact lookup (18-char matching)...")
    contact_lookup_dict = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(contact_lookup_dict)} contact mappings")
    
    print("   • RecordTypeId lookup (Excel with Source/Destination sheets)...")
    map_recordtypeid = load_recordtypeid_lookup(RECORDTYPEID_LOOKUP_FILE)
    print("     ✅ RecordTypeId mapping function ready")
    
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
        "Account_RFPD": [],
        "Primary_Contact_RFPD": [],
        "Purchasing_Contact_RFPD": [],
    }
    
    # === TRACK UNMAPPED RECORDS ===
    unmapped_data = {
        "AccountId": [],
        "Primary_Contact__c": [],
        "Purchasing_Contact__c": [],
        "ContactId": [],
        "RecordTypeId": [],
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
        for col in list(unmapped_data.keys()):
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()
        
        # ================================================================
        # STEP 1: RECORD TYPE BLANKING & CONSTANT MAPPING (BEFORE LOOKUP)
        # ================================================================
        
        # --- AccountId RecordType Handling ---
        if "AccountId" in chunk.columns and "Account.recordtype.Name" in chunk.columns:
            recordtype_col = "Account.recordtype.Name"
            account_col = "AccountId"
            recordtype_vals = chunk[recordtype_col].astype(str).str.strip().str.upper()
            
            # RFPD Account → Blank
            mask_rfpd = recordtype_vals == "RFPD ACCOUNT"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", account_col]].copy()
                blanked_data["Account_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, account_col] = ""
            
            # Unity → Set constant value
            mask_unity = recordtype_vals == "UNITY"
            if mask_unity.any():
                chunk.loc[mask_unity, account_col] = ACCOUNT_UNITY_ID
            
            # Arrow / Verical → Set constant value
            mask_arrow = recordtype_vals == "ARROW / VERICAL"
            if mask_arrow.any():
                chunk.loc[mask_arrow, account_col] = ACCOUNT_ARROW_VERTICAL_ID
        
        # --- Primary_Contact__c RecordType Blanking ---
        if "Primary_Contact__c" in chunk.columns and "Primary_Contact__r.Account.recordtype.Name" in chunk.columns:
            recordtype_col = "Primary_Contact__r.Account.recordtype.Name"
            contact_col = "Primary_Contact__c"
            
            mask_rfpd = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD ACCOUNT"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", contact_col]].copy()
                blanked_data["Primary_Contact_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, contact_col] = ""
        
        # --- Purchasing_Contact__c RecordType Blanking ---
        if "Purchasing_Contact__c" in chunk.columns and "Purchasing_Contact__r.Account.recordtype.Name" in chunk.columns:
            recordtype_col = "Purchasing_Contact__r.Account.recordtype.Name"
            contact_col = "Purchasing_Contact__c"
            
            mask_rfpd = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD ACCOUNT"
            if mask_rfpd.any():
                blanked_rows = chunk.loc[mask_rfpd, ["Id", contact_col]].copy()
                blanked_data["Purchasing_Contact_RFPD"].append(blanked_rows)
                chunk.loc[mask_rfpd, contact_col] = ""
        
        # Update original values after blanking for correct unmapped tracking
        for col in ["AccountId", "Primary_Contact__c", "Purchasing_Contact__c"]:
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
        
        # --- AccountId Lookup (skip if already set by recordtype logic) ---
        if "AccountId" in chunk.columns:
            original = original_values.get("AccountId", pd.Series([""] * len(chunk)))
            
            # Only apply lookup for rows that don't already have a constant value
            def map_account(val):
                val_clean = str(val).strip()
                if not val_clean:
                    return ""
                # Check if it's already a constant value (from recordtype handling)
                if val_clean in [ACCOUNT_UNITY_ID, ACCOUNT_ARROW_VERTICAL_ID]:
                    return val_clean
                return account_lookup_dict.get(val_clean.lower(), "")
            
            chunk["AccountId"] = chunk["AccountId"].apply(map_account)
            
            mapped = chunk["AccountId"].astype(str).str.strip()
            
            # Track unmapped (source had value but mapping failed, excluding constants)
            unmapped_mask = (original != "") & (mapped == "") & \
                            (~original.isin([ACCOUNT_UNITY_ID, ACCOUNT_ARROW_VERTICAL_ID]))
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["AccountId"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["AccountId"].append(unmapped_rows[["Id", "AccountId"]])
        
        # --- Contact Lookups (18-char matching) ---
        contact_fields = ["Primary_Contact__c", "Purchasing_Contact__c", "ContactId"]
        
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
        
        # --- RecordTypeId Mapping (Two-Step via DeveloperName) ---
        if "RecordTypeId" in chunk.columns:
            original = original_values.get("RecordTypeId", pd.Series([""] * len(chunk)))
            
            def apply_recordtypeid_mapping(val):
                val_clean = str(val).strip()
                if not val_clean:
                    return ""
                result = map_recordtypeid(val_clean)
                if result is None:
                    return ""  # Will be tracked as unmapped
                return result
            
            chunk["RecordTypeId"] = chunk["RecordTypeId"].apply(apply_recordtypeid_mapping)
            
            mapped = chunk["RecordTypeId"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["RecordTypeId"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["RecordTypeId"].append(unmapped_rows[["Id", "RecordTypeId"]])
        
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
        "Account_RFPD": "AccountId_Blanked_RFPD.csv",
        "Primary_Contact_RFPD": "Primary_Contact__c_Blanked_RFPD.csv",
        "Purchasing_Contact_RFPD": "Purchasing_Contact__c_Blanked_RFPD.csv",
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
    
    for col in ["Primary_Contact__c", "Purchasing_Contact__c", "ContactId"]:
        if unmapped_data[col]:
            unmapped_df = pd.concat(unmapped_data[col], ignore_index=True)
            contact_ids = unmapped_df[col].astype(str).str.strip()
            
            unmapped_df["In_RFPD"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).lower() in rfpd_contact_ids else "FALSE"
            )
            unmapped_df["In_nullemail"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).lower() in null_email_ids else "FALSE"
            )
            
            verification_file = os.path.join(OUTPUT_DIR, f"{col}_Contact_Verification.csv")
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
    print("✅ OPPORTUNITY MAPPING COMPLETED!")
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

