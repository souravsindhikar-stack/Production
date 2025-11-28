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
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"
DEFAULT_AGENT_MANAGER_ID = "005A0000000rXeVIAU"

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========

# =========================================================================
# THIS SCRIPT USES:
# - 15-CHAR MATCHING FOR Contact_ID__c ONLY (source has 15-char IDs)
# - FULL 18-CHAR MATCHING FOR Recipient_Contact__c (source has 18-char IDs)
# - FULL MATCHING FOR User and Case fields
# =========================================================================


def load_user_lookup(path):
    """Load user lookup file and return dictionary (full ID matching)"""
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


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id", use_15char=False):
    """Load a simple key-value lookup file.
    If use_15char=True, truncates key to first 15 characters for matching.
    """
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
    
    if use_15char:
        # Use first 15 characters of key for matching
        return {
            str(k).strip().lower()[:15]: str(v).strip()
            for k, v in zip(df[key_col], df[value_col])
            if str(k).strip()
        }
    else:
        return {
            str(k).strip().lower(): str(v).strip()
            for k, v in zip(df[key_col], df[value_col])
            if str(k).strip()
        }


def load_id_set(path, id_col="Id", use_15char=False):
    """Load a file and return set of IDs for checking.
    If use_15char=True, stores first 15 characters of each ID.
    """
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
    
    if use_15char:
        return {str(v).strip().lower()[:15] for v in df[id_col] if str(v).strip()}
    else:
        return {str(v).strip().lower() for v in df[id_col] if str(v).strip()}


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    # === LOAD ALL LOOKUP FILES ===
    print("="*70)
    print("📖 LOADING LOOKUP FILES")
    print("="*70)
    
    print("\n• User lookup (full ID matching)...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"  ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("\n• Contact lookup for Contact_ID__c (15-CHAR MATCHING)...")
    contact_lookup_15char = load_simple_lookup(CONTACT_LOOKUP_FILE, use_15char=True)
    print(f"  ✅ Loaded {len(contact_lookup_15char)} contact mappings (15-char keys)")
    
    print("\n• Contact lookup for Recipient_Contact__c (full 18-char matching)...")
    contact_lookup_full = load_simple_lookup(CONTACT_LOOKUP_FILE, use_15char=False)
    print(f"  ✅ Loaded {len(contact_lookup_full)} contact mappings (full keys)")
    
    print("\n• Case lookup (full ID matching)...")
    case_lookup_dict = load_simple_lookup(CASE_LOOKUP_FILE, use_15char=False)
    print(f"  ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("\n• RFPD contact IDs (15-char for Contact_ID__c verification)...")
    rfpd_contact_ids_15char = load_id_set(RFPD_CONTACT_IDS_FILE, "Id", use_15char=True)
    print(f"  ✅ Loaded {len(rfpd_contact_ids_15char)} RFPD contact IDs (15-char)")
    
    print("\n• RFPD contact IDs (full for Recipient_Contact__c verification)...")
    rfpd_contact_ids_full = load_id_set(RFPD_CONTACT_IDS_FILE, "Id", use_15char=False)
    print(f"  ✅ Loaded {len(rfpd_contact_ids_full)} RFPD contact IDs (full)")
    
    print("\n• Null email contacts (15-char for Contact_ID__c verification)...")
    null_email_ids_15char = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id", use_15char=True)
    print(f"  ✅ Loaded {len(null_email_ids_15char)} null email contact IDs (15-char)")
    
    print("\n• Null email contacts (full for Recipient_Contact__c verification)...")
    null_email_ids_full = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id", use_15char=False)
    print(f"  ✅ Loaded {len(null_email_ids_full)} null email contact IDs (full)")
    
    # === PREPARE OUTPUT FILES ===
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_15char_mapped.csv")
    
    if os.path.exists(main_output_file):
        os.remove(main_output_file)
    
    # === TRACK UNMAPPED RECORDS ===
    unmapped_data = {
        "Agent__c": [],
        "Managers_Name_LU__c": [],
        "Case__c": [],
        "Contact_ID__c": [],
        "Recipient_Contact__c": [],
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
        
        # === STANDARD USER LOOKUP (OwnerId, CreatedById, LastModifiedById) ===
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
        
        # === AGENT__C AND MANAGERS_NAME_LU__C USER LOOKUP ===
        custom_user_fields = ["Agent__c", "Managers_Name_LU__c"]
        
        for col in custom_user_fields:
            if col in chunk.columns:
                original = original_values.get(col, pd.Series([""] * len(chunk)))
                
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                mapped = chunk[col].astype(str).str.strip()
                
                unmapped_mask = (original != "") & (mapped == "")
                if unmapped_mask.any():
                    unmapped_rows = chunk[unmapped_mask].copy()
                    unmapped_rows[col] = original[unmapped_mask].values
                    if "Id" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Id", col]])
                    elif "Legacy_SF_Record_ID__c" in chunk.columns:
                        unmapped_data[col].append(unmapped_rows[["Legacy_SF_Record_ID__c", col]])
                    
                    chunk.loc[unmapped_mask, col] = DEFAULT_AGENT_MANAGER_ID
        
        # === CASE__C LOOKUP (full matching) ===
        if "Case__c" in chunk.columns:
            original = original_values.get("Case__c", pd.Series([""] * len(chunk)))
            
            chunk["Case__c"] = chunk["Case__c"].apply(
                lambda val: case_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["Case__c"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["Case__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["Case__c"].append(unmapped_rows[["Id", "Case__c"]])
                elif "Legacy_SF_Record_ID__c" in chunk.columns:
                    unmapped_data["Case__c"].append(unmapped_rows[["Legacy_SF_Record_ID__c", "Case__c"]])
        
        # === CONTACT_ID__C LOOKUP (15-CHAR MATCHING) ===
        if "Contact_ID__c" in chunk.columns:
            original = original_values.get("Contact_ID__c", pd.Series([""] * len(chunk)))
            
            # Use 15-char matching for Contact_ID__c
            chunk["Contact_ID__c"] = chunk["Contact_ID__c"].apply(
                lambda val: contact_lookup_15char.get(str(val).strip().lower()[:15], "") if str(val).strip() else ""
            )
            
            mapped = chunk["Contact_ID__c"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["Contact_ID__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["Contact_ID__c"].append(unmapped_rows[["Id", "Contact_ID__c"]])
                elif "Legacy_SF_Record_ID__c" in chunk.columns:
                    unmapped_data["Contact_ID__c"].append(unmapped_rows[["Legacy_SF_Record_ID__c", "Contact_ID__c"]])
        
        # === RECIPIENT_CONTACT__C LOOKUP (FULL 18-CHAR MATCHING) ===
        if "Recipient_Contact__c" in chunk.columns:
            original = original_values.get("Recipient_Contact__c", pd.Series([""] * len(chunk)))
            
            # Use full matching for Recipient_Contact__c (source has 18-char IDs)
            chunk["Recipient_Contact__c"] = chunk["Recipient_Contact__c"].apply(
                lambda val: contact_lookup_full.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["Recipient_Contact__c"].astype(str).str.strip()
            
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["Recipient_Contact__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["Recipient_Contact__c"].append(unmapped_rows[["Id", "Recipient_Contact__c"]])
                elif "Legacy_SF_Record_ID__c" in chunk.columns:
                    unmapped_data["Recipient_Contact__c"].append(unmapped_rows[["Legacy_SF_Record_ID__c", "Recipient_Contact__c"]])
        
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
    print("\n" + "="*70)
    print("📝 WRITING UNMAPPED REPORTS")
    print("="*70)
    
    unmapped_counts = {}
    
    for col, data_list in unmapped_data.items():
        if data_list:
            unmapped_df = pd.concat(data_list, ignore_index=True)
            unmapped_file = os.path.join(OUTPUT_DIR, f"{col}_15char_unmapped.csv")
            unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
            unmapped_counts[col] = len(unmapped_df)
            print(f"⚠️ {col}: {len(unmapped_df)} unmapped → {unmapped_file}")
        else:
            unmapped_counts[col] = 0
    
    # === CONTACT VERIFICATION FILES ===
    print("\n📝 Generating Contact verification reports...")
    
    # Contact_ID__c verification (15-char matching)
    if unmapped_data["Contact_ID__c"]:
        unmapped_df = pd.concat(unmapped_data["Contact_ID__c"], ignore_index=True)
        contact_ids = unmapped_df["Contact_ID__c"].astype(str).str.strip()
        
        # Use 15-char for verification
        unmapped_df["In_RFPD"] = contact_ids.apply(
            lambda x: "TRUE" if str(x).lower()[:15] in rfpd_contact_ids_15char else "FALSE"
        )
        unmapped_df["In_nullemail"] = contact_ids.apply(
            lambda x: "TRUE" if str(x).lower()[:15] in null_email_ids_15char else "FALSE"
        )
        
        verification_file = os.path.join(OUTPUT_DIR, "Contact_ID__c_15char_verification.csv")
        unmapped_df.to_csv(verification_file, index=False, encoding="utf-8-sig")
        
        in_rfpd_count = (unmapped_df["In_RFPD"] == "TRUE").sum()
        in_nullemail_count = (unmapped_df["In_nullemail"] == "TRUE").sum()
        
        print(f"✅ Contact_ID__c verification (15-char) → {verification_file}")
        print(f"   • In RFPD: {in_rfpd_count}")
        print(f"   • In Null Email: {in_nullemail_count}")
    
    # Recipient_Contact__c verification (full matching)
    if unmapped_data["Recipient_Contact__c"]:
        unmapped_df = pd.concat(unmapped_data["Recipient_Contact__c"], ignore_index=True)
        contact_ids = unmapped_df["Recipient_Contact__c"].astype(str).str.strip()
        
        # Use full matching for verification
        unmapped_df["In_RFPD"] = contact_ids.apply(
            lambda x: "TRUE" if str(x).lower() in rfpd_contact_ids_full else "FALSE"
        )
        unmapped_df["In_nullemail"] = contact_ids.apply(
            lambda x: "TRUE" if str(x).lower() in null_email_ids_full else "FALSE"
        )
        
        verification_file = os.path.join(OUTPUT_DIR, "Recipient_Contact__c_verification.csv")
        unmapped_df.to_csv(verification_file, index=False, encoding="utf-8-sig")
        
        in_rfpd_count = (unmapped_df["In_RFPD"] == "TRUE").sum()
        in_nullemail_count = (unmapped_df["In_nullemail"] == "TRUE").sum()
        
        print(f"✅ Recipient_Contact__c verification (full) → {verification_file}")
        print(f"   • In RFPD: {in_rfpd_count}")
        print(f"   • In Null Email: {in_nullemail_count}")
    
    # === SUMMARY ===
    print("\n" + "="*70)
    print("✅ CASE SURVEYS MAPPING COMPLETED! (15-CHAR FOR Contact_ID__c)")
    print("="*70)
    print(f"📊 Total rows processed: {total_rows:,}")
    print(f"📝 Main output: {main_output_file}")
    
    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")
    
    total_unmapped = sum(unmapped_counts.values())
    print(f"\n⚠️ Total unmapped records across all columns: {total_unmapped:,}")
    
    if total_unmapped > 0:
        print("\nUnmapped breakdown:")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count:,}")
    
    print("\n" + "-"*70)
    print("📋 MATCHING METHODS USED:")
    print("-"*70)
    print("• Contact_ID__c: 15-CHAR matching (source has 15-char IDs)")
    print("• Recipient_Contact__c: Full 18-char matching (source has 18-char IDs)")
    print("• User fields: Full matching")
    print("• Case__c: Full matching")


if __name__ == "__main__":
    main()

