import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Case Surveys data
SOURCE_FILE = r"D:\Production\Source\CaseSurveys_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Contact_Lkp.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\Case_Lkp.csv"

# Additional Contact verification files
NULL_EMAIL_CONTACTS_FILE = r"D:\Production\Lkp Files\NullEmail_Contacts.csv"
RFPD_CONTACT_IDS_FILE = r"D:\Production\Lkp Files\RFPD_Contact_Ids.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
# Default IDs for standard user fields
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# Default ID for Agent__c and Managers_Name_LU__c (same as CreatedById/LastModifiedById)
DEFAULT_AGENT_MANAGER_ID = "005A0000000rXeVIAU"

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_user_lookup(path):
    """Load user lookup file and return simple mapping function"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"User lookup file not found: {path}")
    
    df = pd.read_csv(path, dtype=str).fillna("")
    
    if "Legacy_SF_Record_ID__c" not in df.columns or "Id" not in df.columns:
        raise ValueError(f"User lookup file must contain 'Legacy_SF_Record_ID__c' and 'Id' columns")
    
    df["Legacy_SF_Record_ID__c"] = df["Legacy_SF_Record_ID__c"].astype(str).str.strip()
    df["Id"] = df["Id"].astype(str).str.strip()
    
    lookup_dict = {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df["Legacy_SF_Record_ID__c"], df["Id"])
        if str(k).strip()
    }
    
    return lookup_dict


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
    
    # === LOAD ALL LOOKUP FILES ===
    print("📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Contact lookup...")
    contact_lookup_dict = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(contact_lookup_dict)} contact mappings")
    
    print("   • Case lookup...")
    case_lookup_dict = load_simple_lookup(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("\n📖 Loading contact verification files...")
    print("   • Null email contacts...")
    null_email_ids = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id")
    print(f"     ✅ Loaded {len(null_email_ids)} null email contact IDs")
    
    print("   • RFPD contact IDs...")
    rfpd_contact_ids = load_id_set(RFPD_CONTACT_IDS_FILE, "Id")
    print(f"     ✅ Loaded {len(rfpd_contact_ids)} RFPD contact IDs")
    
    # === PREPARE OUTPUT FILES ===
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")
    
    if os.path.exists(main_output_file):
        os.remove(main_output_file)
    
    # === TRACK UNMAPPED RECORDS ===
    unmapped_data = {
        # User fields
        "Agent__c": [],
        "Managers_Name_LU__c": [],
        # Case field
        "Case__c": [],
        # Contact fields
        "Contact_ID__c": [],
        "Recipient_Contact__c": [],
    }
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0
    
    print("\n🔄 Processing source file in chunks...")
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        
        # Store original values for tracking
        original_values = {}
        for col in unmapped_data.keys():
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()
        
        # === STANDARD USER LOOKUP (OwnerId, CreatedById, LastModifiedById) ===
        # Logic: Always apply default if blank or unmapped (NO unmapped files for these)
        standard_user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]
        
        for col in standard_user_fields:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                # Apply defaults for blank/unmapped values
                if col == "OwnerId":
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_OWNER_ID
                else:  # CreatedById, LastModifiedById
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID
        
        # === AGENT__C AND MANAGERS_NAME_LU__C USER LOOKUP ===
        # Logic: Blank stays blank, non-blank unmapped gets default + tracked in unmapped file
        custom_user_fields = ["Agent__c", "Managers_Name_LU__c"]
        
        for col in custom_user_fields:
            if col in chunk.columns:
                original = original_values.get(col, pd.Series([""] * len(chunk)))
                
                # Apply lookup (blank values stay blank)
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                mapped = chunk[col].astype(str).str.strip()
                
                # Track unmapped: original had NON-BLANK value but mapping returned blank
                unmapped_mask = (original != "") & (mapped == "")
                if unmapped_mask.any():
                    # Save unmapped records BEFORE applying default
                    unmapped_rows = chunk[unmapped_mask].copy()
                    unmapped_rows[col] = original[unmapped_mask].values
                    if "Id" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Id", col]])
                    elif "Legacy_SF_Record_ID__c" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Legacy_SF_Record_ID__c", col]])
                    
                    # Set default ONLY for unmapped (source had value but couldn't map)
                    chunk.loc[unmapped_mask, col] = DEFAULT_AGENT_MANAGER_ID
                
                # NOTE: Blank source values remain blank (no default applied)
        
        # === CASE__C LOOKUP ===
        if "Case__c" in chunk.columns:
            original = original_values.get("Case__c", pd.Series([""] * len(chunk)))
            
            chunk["Case__c"] = chunk["Case__c"].apply(
                lambda val: case_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["Case__c"].astype(str).str.strip()
            
            # Track unmapped
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["Case__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["Case__c"].append(unmapped_rows[["Id", "Case__c"]])
                elif "Legacy_SF_Record_ID__c" in chunk.columns:
                    unmapped_data["Case__c"].append(unmapped_rows[["Legacy_SF_Record_ID__c", "Case__c"]])
        
        # === CONTACT LOOKUPS (Contact_ID__c, Recipient_Contact__c) ===
        contact_fields = ["Contact_ID__c", "Recipient_Contact__c"]
        
        for col in contact_fields:
            if col in chunk.columns:
                original = original_values.get(col, pd.Series([""] * len(chunk)))
                
                chunk[col] = chunk[col].apply(
                    lambda val: contact_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                mapped = chunk[col].astype(str).str.strip()
                
                # Track unmapped
                unmapped_mask = (original != "") & (mapped == "")
                if unmapped_mask.any():
                    unmapped_rows = chunk[unmapped_mask].copy()
                    unmapped_rows[col] = original[unmapped_mask].values
                    if "Id" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Id", col]])
                    elif "Legacy_SF_Record_ID__c" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Legacy_SF_Record_ID__c", col]])
        
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
        
        print(f"✅ Chunk {chunk_idx}: {len(chunk)} rows processed")
    
    # === WRITE UNMAPPED FILES ===
    print("\n📝 Writing unmapped reports...")
    unmapped_counts = {}
    
    # Determine ID column name
    id_col_name = "Legacy_SF_Record_ID__c" if any("Legacy_SF_Record_ID__c" in str(df.columns.tolist()) for df in unmapped_data.get("Case__c", [pd.DataFrame()]) if len(df) > 0) else "Id"
    
    for col, data_list in unmapped_data.items():
        if data_list:
            unmapped_df = pd.concat(data_list, ignore_index=True)
            unmapped_file = os.path.join(OUTPUT_DIR, f"{col}_unmapped.csv")
            unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
            unmapped_counts[col] = len(unmapped_df)
            print(f"   ⚠️ {col}: {len(unmapped_df)} unmapped → {unmapped_file}")
        else:
            unmapped_counts[col] = 0
    
    # === CONTACT VERIFICATION: Check unmapped contacts against RFPD and Null Email files ===
    print("\n📝 Generating Contact verification reports...")
    
    for contact_col in ["Contact_ID__c", "Recipient_Contact__c"]:
        if unmapped_data[contact_col]:
            unmapped_contacts_df = pd.concat(unmapped_data[contact_col], ignore_index=True)
            
            # Get the contact IDs (the unmapped values)
            contact_ids = unmapped_contacts_df[contact_col].astype(str).str.strip()
            
            # Check against RFPD and Null Email files
            unmapped_contacts_df["In_RFPD"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).strip().lower() in rfpd_contact_ids else "FALSE"
            )
            unmapped_contacts_df["In_nullemail"] = contact_ids.apply(
                lambda x: "TRUE" if str(x).strip().lower() in null_email_ids else "FALSE"
            )
            
            # Save verification file
            verification_file = os.path.join(OUTPUT_DIR, f"{contact_col}_verification.csv")
            unmapped_contacts_df.to_csv(verification_file, index=False, encoding="utf-8-sig")
            
            # Count stats
            in_rfpd_count = (unmapped_contacts_df["In_RFPD"] == "TRUE").sum()
            in_nullemail_count = (unmapped_contacts_df["In_nullemail"] == "TRUE").sum()
            
            print(f"   ✅ {contact_col} verification → {verification_file}")
            print(f"      • In RFPD: {in_rfpd_count}")
            print(f"      • In Null Email: {in_nullemail_count}")
    
    # === SUMMARY ===
    print("\n" + "="*60)
    print("✅ CASE SURVEYS MAPPING COMPLETED!")
    print("="*60)
    print(f"📊 Total rows processed: {total_rows}")
    print(f"📝 Main output: {main_output_file}")
    
    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")
    
    total_unmapped = sum(unmapped_counts.values())
    print(f"\n⚠️ Total unmapped records across all columns: {total_unmapped}")
    
    if total_unmapped > 0:
        print("\nUnmapped breakdown:")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count}")


if __name__ == "__main__":
    main()

