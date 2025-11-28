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

# ========= END OF USER INPUTS =========

# =========================================================================
# THIS SCRIPT USES 15-CHARACTER MATCHING FOR CONTACT LOOKUPS
# Source file has 15-char IDs, Lookup files have 18-char IDs
# We truncate lookup file IDs to 15 chars for matching
# =========================================================================


def load_lookup_dict(path, key_col="Legacy_SF_Record_ID__c", value_col="Id", use_15char=False):
    """Load a lookup file and return dictionary.
    If use_15char=True, truncates key to first 15 characters for matching.
    """
    if not os.path.exists(path):
        print(f"   ⚠️ File not found: {path}")
        return {}
    
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    
    df = df.fillna("")
    
    if key_col not in df.columns or value_col not in df.columns:
        print(f"   ⚠️ Required columns not found in {path}")
        return {}
    
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    
    if use_15char:
        # Use first 15 characters of key for matching (15-char to 18-char matching)
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
        # Use first 15 characters for matching
        return {str(v).strip().lower()[:15] for v in df[id_col] if str(v).strip()}
    else:
        return {str(v).strip().lower() for v in df[id_col] if str(v).strip()}


def create_lkp_and_flag(df, col, lookup_dict, use_15char=False):
    """Create _Lkp and _Flag columns for a field.
    If use_15char=True, uses first 15 characters of source value for lookup.
    Flag = Y if Lkp has value, N if source has value but Lkp is empty, blank if source is empty.
    """
    if use_15char:
        # Use first 15 characters for lookup
        df[f"{col}_Lkp"] = df[col].apply(
            lambda x: lookup_dict.get(str(x).strip().lower()[:15], "") if str(x).strip() else ""
        )
    else:
        df[f"{col}_Lkp"] = df[col].apply(
            lambda x: lookup_dict.get(str(x).strip().lower(), "") if str(x).strip() else ""
        )
    
    def get_flag(row):
        source_val = str(row[col]).strip()
        lkp_val = str(row[f"{col}_Lkp"]).strip()
        
        if not source_val:
            return ""  # Source is blank
        elif lkp_val:
            return "Y"  # Matched
        else:
            return "N"  # Source has value but no match
    
    df[f"{col}_Flag"] = df.apply(get_flag, axis=1)
    
    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    # === LOAD ALL LOOKUP FILES ===
    print("="*70)
    print("📖 LOADING LOOKUP FILES (15-CHAR MATCHING FOR CONTACTS)")
    print("="*70)
    
    print("\n• User lookup (full ID matching)...")
    user_lookup_dict = load_lookup_dict(USER_LOOKUP_FILE, use_15char=False)
    print(f"  ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("\n• Contact lookup (15-CHAR MATCHING)...")
    contact_lookup_dict = load_lookup_dict(CONTACT_LOOKUP_FILE, use_15char=True)
    print(f"  ✅ Loaded {len(contact_lookup_dict)} contact mappings (using first 15 chars)")
    
    print("\n• Case lookup (full ID matching)...")
    case_lookup_dict = load_lookup_dict(CASE_LOOKUP_FILE, use_15char=False)
    print(f"  ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("\n• Null email contacts (15-CHAR MATCHING)...")
    null_email_ids = load_id_set(NULL_EMAIL_CONTACTS_FILE, "Id", use_15char=True)
    print(f"  ✅ Loaded {len(null_email_ids)} null email contact IDs (using first 15 chars)")
    
    print("\n• RFPD contact IDs (15-CHAR MATCHING)...")
    rfpd_contact_ids = load_id_set(RFPD_CONTACT_IDS_FILE, "Id", use_15char=True)
    print(f"  ✅ Loaded {len(rfpd_contact_ids)} RFPD contact IDs (using first 15 chars)")
    
    # === LOAD SOURCE FILE ===
    print("\n" + "="*70)
    print("📖 LOADING SOURCE FILE")
    print("="*70)
    
    df = pd.read_csv(SOURCE_FILE, dtype=str, encoding='utf-8-sig')
    df = df.fillna("")
    total_records = len(df)
    print(f"\n✅ Loaded {total_records:,} total records")
    
    # Rename Id column if present
    if "Id" in df.columns:
        df.rename(columns={'Id': 'Legacy_SF_Record_ID__c'}, inplace=True)
    
    # === AUDIT DATA STRUCTURES ===
    audit_summary = {}
    unmapped_user_data = {}
    
    # === AUDIT STANDARD USER FIELDS ===
    print("\n" + "="*70)
    print("🔍 AUDITING STANDARD USER FIELDS")
    print("="*70)
    
    standard_user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]
    
    for col in standard_user_fields:
        if col in df.columns:
            df = create_lkp_and_flag(df, col, user_lookup_dict, use_15char=False)
            
            stripped_col = df[col].astype(str).str.strip()
            non_blank_mask = stripped_col != ""
            total_non_blank = non_blank_mask.sum()
            unique_count = stripped_col[non_blank_mask].nunique()
            
            matched = (df[f"{col}_Flag"] == "Y").sum()
            unmatched = (df[f"{col}_Flag"] == "N").sum()
            
            unmatched_stripped = stripped_col[df[f"{col}_Flag"] == "N"]
            unique_unmatched = unmatched_stripped.nunique()
            unmatched_values = unmatched_stripped.unique()
            
            if unmatched > 0:
                unmapped_user_data[col] = {
                    "unique_values": list(unmatched_values),
                    "default_id": DEFAULT_OWNER_ID if col == "OwnerId" else DEFAULT_CREATEDBY_LASTMODIFIED_ID,
                    "records": df.loc[df[f"{col}_Flag"] == "N", ["Legacy_SF_Record_ID__c", col]].copy()
                }
            
            audit_summary[col] = {
                "total_non_blank": total_non_blank,
                "unique_count": unique_count,
                "matched": matched,
                "unmatched": unmatched,
                "unique_unmatched": unique_unmatched,
                "default_id": DEFAULT_OWNER_ID if col == "OwnerId" else DEFAULT_CREATEDBY_LASTMODIFIED_ID
            }
            
            print(f"\n• {col}:")
            print(f"  Total non-blank records: {total_non_blank:,}")
            print(f"  Unique values: {unique_count:,}")
            print(f"  Matched: {matched:,}")
            print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
            if unmatched > 0:
                default_id = DEFAULT_OWNER_ID if col == "OwnerId" else DEFAULT_CREATEDBY_LASTMODIFIED_ID
                print(f"  → Will use default: {default_id}")
        else:
            print(f"\n• {col}: Column not found in source")
    
    # === AUDIT CUSTOM USER FIELDS ===
    print("\n" + "="*70)
    print("🔍 AUDITING CUSTOM USER FIELDS (Agent__c, Managers_Name_LU__c)")
    print("="*70)
    
    custom_user_fields = ["Agent__c", "Managers_Name_LU__c"]
    
    for col in custom_user_fields:
        if col in df.columns:
            df = create_lkp_and_flag(df, col, user_lookup_dict, use_15char=False)
            
            stripped_col = df[col].astype(str).str.strip()
            non_blank_mask = stripped_col != ""
            total_non_blank = non_blank_mask.sum()
            unique_count = stripped_col[non_blank_mask].nunique()
            
            matched = (df[f"{col}_Flag"] == "Y").sum()
            unmatched = (df[f"{col}_Flag"] == "N").sum()
            
            unmatched_stripped = stripped_col[df[f"{col}_Flag"] == "N"]
            unique_unmatched = unmatched_stripped.nunique()
            unmatched_values = unmatched_stripped.unique()
            
            if unmatched > 0:
                unmapped_user_data[col] = {
                    "unique_values": list(unmatched_values),
                    "default_id": DEFAULT_AGENT_MANAGER_ID,
                    "records": df.loc[df[f"{col}_Flag"] == "N", ["Legacy_SF_Record_ID__c", col]].copy()
                }
            
            audit_summary[col] = {
                "total_non_blank": total_non_blank,
                "unique_count": unique_count,
                "matched": matched,
                "unmatched": unmatched,
                "unique_unmatched": unique_unmatched,
                "default_id": DEFAULT_AGENT_MANAGER_ID
            }
            
            print(f"\n• {col}:")
            print(f"  Total non-blank records: {total_non_blank:,}")
            print(f"  Unique values: {unique_count:,}")
            print(f"  Matched: {matched:,}")
            print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
            if unmatched > 0:
                print(f"  → Will use default: {DEFAULT_AGENT_MANAGER_ID}")
        else:
            print(f"\n• {col}: Column not found in source")
    
    # === AUDIT CASE FIELD ===
    print("\n" + "="*70)
    print("🔍 AUDITING CASE FIELD (Case__c)")
    print("="*70)
    
    if "Case__c" in df.columns:
        df = create_lkp_and_flag(df, "Case__c", case_lookup_dict, use_15char=False)
        
        stripped_col = df["Case__c"].astype(str).str.strip()
        non_blank_mask = stripped_col != ""
        total_non_blank = non_blank_mask.sum()
        unique_count = stripped_col[non_blank_mask].nunique()
        
        matched = (df["Case__c_Flag"] == "Y").sum()
        unmatched = (df["Case__c_Flag"] == "N").sum()
        
        unmatched_stripped = stripped_col[df["Case__c_Flag"] == "N"]
        unique_unmatched = unmatched_stripped.nunique()
        
        audit_summary["Case__c"] = {
            "total_non_blank": total_non_blank,
            "unique_count": unique_count,
            "matched": matched,
            "unmatched": unmatched,
            "unique_unmatched": unique_unmatched
        }
        
        print(f"\n• Case__c:")
        print(f"  Total non-blank records: {total_non_blank:,}")
        print(f"  Unique values: {unique_count:,}")
        print(f"  Matched: {matched:,}")
        print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
    else:
        print("\n• Case__c: Column not found in source")
    
    # === AUDIT CONTACT FIELDS (15-CHAR MATCHING + RFPD and Null Email check) ===
    print("\n" + "="*70)
    print("🔍 AUDITING CONTACT FIELDS (15-CHAR MATCHING)")
    print("="*70)
    
    contact_fields = ["Contact_ID__c", "Recipient_Contact__c"]
    contact_verification_data = {}
    
    for col in contact_fields:
        if col in df.columns:
            # Use 15-character matching for contacts
            df = create_lkp_and_flag(df, col, contact_lookup_dict, use_15char=True)
            
            stripped_col = df[col].astype(str).str.strip()
            non_blank_mask = stripped_col != ""
            total_non_blank = non_blank_mask.sum()
            unique_count = stripped_col[non_blank_mask].nunique()
            
            matched = (df[f"{col}_Flag"] == "Y").sum()
            unmatched = (df[f"{col}_Flag"] == "N").sum()
            
            unmatched_mask = df[f"{col}_Flag"] == "N"
            unmatched_stripped = stripped_col[unmatched_mask]
            unique_unmatched = unmatched_stripped.nunique()
            unmatched_values = unmatched_stripped.unique()
            
            # Check unmatched against RFPD and Null Email using 15-char matching
            # Source values are already 15-char, RFPD/NullEmail sets contain 15-char keys
            in_rfpd_unique = sum(1 for v in unmatched_values if str(v).lower()[:15] in rfpd_contact_ids)
            in_nullemail_unique = sum(1 for v in unmatched_values if str(v).lower()[:15] in null_email_ids)
            in_neither_unique = sum(1 for v in unmatched_values 
                                    if str(v).lower()[:15] not in rfpd_contact_ids 
                                    and str(v).lower()[:15] not in null_email_ids)
            
            unmatched_records = unmatched_stripped.tolist()
            total_in_rfpd = sum(1 for v in unmatched_records if str(v).lower()[:15] in rfpd_contact_ids)
            total_in_nullemail = sum(1 for v in unmatched_records if str(v).lower()[:15] in null_email_ids)
            total_in_neither = sum(1 for v in unmatched_records 
                                   if str(v).lower()[:15] not in rfpd_contact_ids 
                                   and str(v).lower()[:15] not in null_email_ids)
            
            contact_verification_data[col] = {
                "unmatched_unique_values": list(unmatched_values),
                "in_rfpd_unique": in_rfpd_unique,
                "in_nullemail_unique": in_nullemail_unique,
                "in_neither_unique": in_neither_unique,
                "total_in_rfpd": total_in_rfpd,
                "total_in_nullemail": total_in_nullemail,
                "total_in_neither": total_in_neither
            }
            
            audit_summary[col] = {
                "total_non_blank": total_non_blank,
                "unique_count": unique_count,
                "matched": matched,
                "unmatched": unmatched,
                "unique_unmatched": unique_unmatched,
                "in_rfpd_unique": in_rfpd_unique,
                "in_nullemail_unique": in_nullemail_unique,
                "in_neither_unique": in_neither_unique,
                "total_in_rfpd": total_in_rfpd,
                "total_in_nullemail": total_in_nullemail,
                "total_in_neither": total_in_neither
            }
            
            print(f"\n• {col} (15-CHAR MATCHING):")
            print(f"  Total non-blank records: {total_non_blank:,}")
            print(f"  Unique values: {unique_count:,}")
            print(f"  Matched: {matched:,}")
            print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
            print(f"\n  UNMATCHED BREAKDOWN (Total Records):")
            print(f"    In RFPD: {total_in_rfpd:,}")
            print(f"    In Null Email: {total_in_nullemail:,}")
            print(f"    In Neither (Need Investigation): {total_in_neither:,}")
            print(f"\n  UNMATCHED BREAKDOWN (Unique Values):")
            print(f"    In RFPD: {in_rfpd_unique:,}")
            print(f"    In Null Email: {in_nullemail_unique:,}")
            print(f"    In Neither (Need Investigation): {in_neither_unique:,}")
        else:
            print(f"\n• {col}: Column not found in source")
    
    # === BUILD DETAIL REPORT ===
    print("\n" + "="*70)
    print("📝 GENERATING REPORTS")
    print("="*70)
    
    detail_fields = ["Legacy_SF_Record_ID__c"]
    
    for col in standard_user_fields + custom_user_fields:
        if col in df.columns:
            detail_fields.extend([col, f"{col}_Lkp", f"{col}_Flag"])
    
    if "Case__c" in df.columns:
        detail_fields.extend(["Case__c", "Case__c_Lkp", "Case__c_Flag"])
    
    for col in contact_fields:
        if col in df.columns:
            detail_fields.extend([col, f"{col}_Lkp", f"{col}_Flag"])
    
    existing_fields = [f for f in detail_fields if f in df.columns]
    detail_df = df[existing_fields]
    
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_15char_DetailReport.csv")
    detail_df.to_csv(detail_csv, index=False, encoding="utf-8-sig")
    print(f"\n✅ Detail report → {detail_csv}")
    
    # === BUILD SUMMARY REPORT ===
    summary_rows = []
    for field, stats in audit_summary.items():
        row = {
            "Field": field,
            "Total_NonBlank": stats.get("total_non_blank", 0),
            "Unique_Values": stats.get("unique_count", 0),
            "Matched": stats.get("matched", 0),
            "Unmatched_Total": stats.get("unmatched", 0),
            "Unmatched_Unique": stats.get("unique_unmatched", 0),
        }
        
        if field in contact_fields:
            row["In_RFPD_Total"] = stats.get("total_in_rfpd", "N/A")
            row["In_NullEmail_Total"] = stats.get("total_in_nullemail", "N/A")
            row["In_Neither_Total"] = stats.get("total_in_neither", "N/A")
            row["In_RFPD_Unique"] = stats.get("in_rfpd_unique", "N/A")
            row["In_NullEmail_Unique"] = stats.get("in_nullemail_unique", "N/A")
            row["In_Neither_Unique"] = stats.get("in_neither_unique", "N/A")
        else:
            row["In_RFPD_Total"] = "N/A"
            row["In_NullEmail_Total"] = "N/A"
            row["In_Neither_Total"] = "N/A"
            row["In_RFPD_Unique"] = "N/A"
            row["In_NullEmail_Unique"] = "N/A"
            row["In_Neither_Unique"] = "N/A"
        
        summary_rows.append(row)
    
    summary_df = pd.DataFrame(summary_rows)
    summary_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_15char_SummaryReport.csv")
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Summary report → {summary_csv}")
    
    # === GENERATE CONTACT VERIFICATION FILES ===
    for col, data in contact_verification_data.items():
        if data["unmatched_unique_values"]:
            verif_rows = []
            for v in data["unmatched_unique_values"]:
                verif_rows.append({
                    col: v,
                    "In_RFPD": "TRUE" if str(v).lower()[:15] in rfpd_contact_ids else "FALSE",
                    "In_NullEmail": "TRUE" if str(v).lower()[:15] in null_email_ids else "FALSE"
                })
            
            verif_df = pd.DataFrame(verif_rows)
            verif_csv = os.path.join(OUTPUT_DIR, f"{col}_15char_UnmatchedVerification.csv")
            verif_df.to_csv(verif_csv, index=False, encoding="utf-8-sig")
            print(f"✅ {col} verification → {verif_csv}")
    
    # === GENERATE UNMAPPED USER FILES ===
    print("\n📝 Generating unmapped user reports (will receive defaults)...")
    
    for col, data in unmapped_user_data.items():
        if data["unique_values"]:
            unique_rows = []
            for v in data["unique_values"]:
                unique_rows.append({
                    f"Unmapped_{col}": v,
                    "Will_Be_Replaced_With": data["default_id"]
                })
            
            unique_df = pd.DataFrame(unique_rows)
            unique_csv = os.path.join(OUTPUT_DIR, f"{col}_UnmappedUsers_Unique.csv")
            unique_df.to_csv(unique_csv, index=False, encoding="utf-8-sig")
            print(f"✅ {col} unique unmapped users → {unique_csv} ({len(unique_rows):,} unique IDs)")
            
            records_df = data["records"].copy()
            records_df["Will_Be_Replaced_With"] = data["default_id"]
            records_csv = os.path.join(OUTPUT_DIR, f"{col}_UnmappedUsers_AllRecords.csv")
            records_df.to_csv(records_csv, index=False, encoding="utf-8-sig")
            print(f"✅ {col} all unmapped records → {records_csv} ({len(records_df):,} records)")
    
    # === FINAL SUMMARY ===
    print("\n" + "="*70)
    print("📋 AUDIT SUMMARY (15-CHAR MATCHING FOR CONTACTS)")
    print("="*70)
    print(f"\n📊 TOTAL RECORDS IN SOURCE: {total_records:,}")
    print("\n" + "-"*70)
    
    all_user_fields = standard_user_fields + custom_user_fields
    
    for field, stats in audit_summary.items():
        unmatched = stats.get("unmatched", 0)
        unique_unmatched = stats.get("unique_unmatched", 0)
        
        if unmatched > 0:
            print(f"⚠️ {field}:")
            print(f"   Unmatched: {unmatched:,} records (Unique: {unique_unmatched:,})")
            
            if field in all_user_fields:
                default_id = stats.get("default_id", "N/A")
                print(f"   → Will be replaced with DEFAULT: {default_id}")
            
            if field in contact_fields:
                print(f"   → In RFPD: {stats.get('total_in_rfpd', 0):,} records (Unique: {stats.get('in_rfpd_unique', 0):,})")
                print(f"   → In Null Email: {stats.get('total_in_nullemail', 0):,} records (Unique: {stats.get('in_nullemail_unique', 0):,})")
                print(f"   → Need Investigation: {stats.get('total_in_neither', 0):,} records (Unique: {stats.get('in_neither_unique', 0):,})")
        else:
            print(f"✅ {field}: All matched")
    
    print("\n" + "="*70)
    print("✅ AUDIT COMPLETED! (15-CHAR MATCHING FOR CONTACTS)")
    print("="*70)


if __name__ == "__main__":
    main()

