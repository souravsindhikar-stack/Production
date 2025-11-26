import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Individual data
SOURCE_FILE = r"D:\Production\Source\Individual_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Contact_Lkp.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
# Default IDs for user fields (for reference only, not used in audit)
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# ========= END OF USER INPUTS =========


def load_user_lookup(path):
    """Load user lookup file and return mapping dictionary"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"User lookup file not found: {path}")
    
    df = pd.read_csv(path, dtype=str).fillna("")
    
    # Check for required columns
    if "Legacy_SF_Record_ID__c" not in df.columns or "Id" not in df.columns:
        raise ValueError(f"User lookup file must contain 'Legacy_SF_Record_ID__c' and 'Id' columns")
    
    # Strip whitespace from columns
    df["Legacy_SF_Record_ID__c"] = df["Legacy_SF_Record_ID__c"].astype(str).str.strip()
    df["Id"] = df["Id"].astype(str).str.strip()
    
    # Build simple mapping dictionary: Legacy_SF_Record_ID__c -> Id
    lookup_dict = {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df["Legacy_SF_Record_ID__c"], df["Id"])
        if str(k).strip()
    }
    
    return lookup_dict


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a simple key-value lookup file"""
    if not os.path.exists(path):
        print(f"⚠️  Lookup file not found: {path}")
        return {}
    
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
    
    print("📖 Loading user lookup file...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"   ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("\n📖 Loading Contact lookup file...")
    contact_lookup_dict = load_simple_lookup(CONTACT_LOOKUP_FILE)
    if contact_lookup_dict:
        print(f"   ✅ Loaded {len(contact_lookup_dict)} contact mappings")
    else:
        print("   ⚠️  No contact mappings loaded")
    
    # === STEP 1: Load main file ===
    print("\n📖 Loading source file...")
    df = pd.read_csv(SOURCE_FILE, dtype=str, encoding='utf-8-sig')
    df = df.fillna("")
    print(f"   ✅ Loaded {len(df)} records")
    
    # Rename Id column to Legacy_SF_Record_ID__c for clarity
    if "Id" in df.columns:
        df.rename(columns={'Id': 'Legacy_SF_Record_ID__c'}, inplace=True)
    
    # === STEP 2: Create Lkp + Flag fields for User fields ===
    print("\n🔍 Auditing user fields...")
    user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]
    
    for col in user_fields:
        if col in df.columns:
            # Create _Lkp column (mapped value)
            df[f"{col}_Lkp"] = df[col].apply(
                lambda x: user_lookup_dict.get(str(x).strip().lower(), "") if str(x).strip() else ""
            )
            
            # Create _Flag column (Y if found, N if not found)
            df[f"{col}_Flag"] = df[col].apply(
                lambda x: "Y" if str(x).strip().lower() in user_lookup_dict else ("N" if str(x).strip() else "")
            )
            
            matched = (df[f"{col}_Flag"] == "Y").sum()
            unmatched = (df[f"{col}_Flag"] == "N").sum()
            print(f"   • {col}: {matched} matched, {unmatched} unmatched")
        else:
            print(f"   ⚠️ {col}: Column not found in source")
    
    # === STEP 3: Create Lkp + Flag fields for Contact_Origin__c ===
    print("\n🔍 Auditing Contact_Origin__c...")
    if "Contact_Origin__c" in df.columns:
        # Create _Lkp column (mapped value)
        df["Contact_Origin__c_Lkp"] = df["Contact_Origin__c"].apply(
            lambda x: contact_lookup_dict.get(str(x).strip().lower(), "") if str(x).strip() else ""
        )
        
        # Create _Flag column (Y if found, N if not found)
        df["Contact_Origin__c_Flag"] = df["Contact_Origin__c"].apply(
            lambda x: "Y" if str(x).strip().lower() in contact_lookup_dict else ("N" if str(x).strip() else "")
        )
        
        matched = (df["Contact_Origin__c_Flag"] == "Y").sum()
        unmatched = (df["Contact_Origin__c_Flag"] == "N").sum()
        print(f"   • Contact_Origin__c: {matched} matched, {unmatched} unmatched")
    else:
        print("   ⚠️ Contact_Origin__c: Column not found in source")
    
    # === STEP 4: Build Summary counts ===
    print("\n📊 Building summary report...")
    summary_data = {}
    
    for col in user_fields:
        if col in df.columns:
            summary_data[col] = (df[f"{col}_Flag"] == "N").sum()
    
    if "Contact_Origin__c" in df.columns:
        summary_data["Contact_Origin__c"] = (df["Contact_Origin__c_Flag"] == "N").sum()
    
    summary_df = pd.DataFrame(list(summary_data.items()), columns=["Field", "UnmatchedCount"])
    
    # === STEP 5: Select fields for DetailReport ===
    detail_fields = ["Legacy_SF_Record_ID__c"]
    
    # Add user fields
    for col in user_fields:
        if col in df.columns:
            detail_fields.extend([col, f"{col}_Lkp", f"{col}_Flag"])
    
    # Add Contact_Origin__c fields
    if "Contact_Origin__c" in df.columns:
        detail_fields.extend(["Contact_Origin__c", "Contact_Origin__c_Lkp", "Contact_Origin__c_Flag"])
    
    # Create detail dataframe with only the fields that exist
    existing_fields = [f for f in detail_fields if f in df.columns]
    detail_df = df[existing_fields]
    
    # === STEP 6: Save reports as CSV ===
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_DetailReport.csv")
    summary_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_SummaryReport.csv")
    
    detail_df.to_csv(detail_csv, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    
    print("\n" + "="*60)
    print("✅ AUDIT REPORTS GENERATED!")
    print("="*60)
    print(f"📁 Detail report → {detail_csv}")
    print(f"📁 Summary report → {summary_csv}")
    
    # === STEP 7: Display summary ===
    print("\n" + "-"*60)
    print("📋 AUDIT SUMMARY:")
    print("-"*60)
    total_unmapped = summary_df["UnmatchedCount"].sum()
    print(f"Total unmapped records: {total_unmapped}")
    
    if not summary_df.empty:
        for _, row in summary_df.iterrows():
            if row["UnmatchedCount"] > 0:
                print(f"  ⚠️ {row['Field']}: {row['UnmatchedCount']} unmapped")
            else:
                print(f"  ✅ {row['Field']}: All mapped")
    
    if total_unmapped > 0:
        print("\n⚠️  Please review DetailReport and update lookup files before running mapping script!")
    else:
        print("\n✅ All values can be mapped successfully! Ready to run mapping script.")


if __name__ == "__main__":
    main()

