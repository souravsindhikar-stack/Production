import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Case Survey Junction data
SOURCE_FILE = r"D:\Production\Source\CaseSurveyJunction_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\Case_Lkp.csv"
CASE_SURVEY_LOOKUP_FILE = r"D:\Production\Lkp Files\CaseSurvey_Lkp.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# ========= END OF USER INPUTS =========


def load_lookup_dict(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a lookup file and return dictionary"""
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
    
    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df[key_col], df[value_col])
        if str(k).strip()
    }


def create_lkp_and_flag(df, col, lookup_dict):
    """Create _Lkp and _Flag columns for a field.
    Flag = Y if Lkp has value, N if source has value but Lkp is empty, blank if source is empty.
    """
    df[f"{col}_Lkp"] = df[col].apply(
        lambda x: lookup_dict.get(str(x).strip().lower(), "") if str(x).strip() else ""
    )
    
    def get_flag(row):
        source_val = str(row[col]).strip()
        lkp_val = str(row[f"{col}_Lkp"]).strip()
        
        if not source_val:
            return ""
        elif lkp_val:
            return "Y"
        else:
            return "N"
    
    df[f"{col}_Flag"] = df.apply(get_flag, axis=1)
    
    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    print("="*70)
    print("📖 CASE SURVEY JUNCTION - AUDIT")
    print("="*70)
    
    # === LOAD LOOKUP FILES ===
    print("\n📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_lookup_dict(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Case lookup...")
    case_lookup_dict = load_lookup_dict(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("   • Case Survey lookup...")
    case_survey_lookup_dict = load_lookup_dict(CASE_SURVEY_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_survey_lookup_dict)} case survey mappings")
    
    # === LOAD SOURCE FILE ===
    print("\n📖 Loading source file...")
    df = pd.read_csv(SOURCE_FILE, dtype=str, encoding='utf-8-sig')
    df = df.fillna("")
    total_records = len(df)
    print(f"   ✅ Loaded {total_records:,} total records")
    print(f"   📋 Columns: {len(df.columns)}")
    
    # Rename Id column if present
    if "Id" in df.columns:
        df.rename(columns={'Id': 'Legacy_SF_Record_ID__c'}, inplace=True)
    
    # === AUDIT DATA STRUCTURES ===
    audit_summary = {}
    unmapped_user_data = {}
    
    # === AUDIT USER FIELDS ===
    print("\n" + "="*70)
    print("🔍 AUDITING USER FIELDS (CreatedById, LastModifiedById)")
    print("="*70)
    
    user_fields = ["CreatedById", "LastModifiedById"]
    
    for col in user_fields:
        if col in df.columns:
            df = create_lkp_and_flag(df, col, user_lookup_dict)
            
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
                    "default_id": DEFAULT_CREATEDBY_LASTMODIFIED_ID,
                    "records": df.loc[df[f"{col}_Flag"] == "N", ["Legacy_SF_Record_ID__c", col]].copy()
                }
            
            audit_summary[col] = {
                "total_non_blank": total_non_blank,
                "unique_count": unique_count,
                "matched": matched,
                "unmatched": unmatched,
                "unique_unmatched": unique_unmatched,
                "default_id": DEFAULT_CREATEDBY_LASTMODIFIED_ID
            }
            
            print(f"\n• {col}:")
            print(f"  Total non-blank records: {total_non_blank:,}")
            print(f"  Unique values: {unique_count:,}")
            print(f"  Matched: {matched:,}")
            print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
            if unmatched > 0:
                print(f"  → Will use default: {DEFAULT_CREATEDBY_LASTMODIFIED_ID}")
        else:
            print(f"\n• {col}: Column not found in source")
    
    # === AUDIT CASE FIELD ===
    print("\n" + "="*70)
    print("🔍 AUDITING CASE FIELD (Case__c)")
    print("="*70)
    
    if "Case__c" in df.columns:
        df = create_lkp_and_flag(df, "Case__c", case_lookup_dict)
        
        stripped_col = df["Case__c"].astype(str).str.strip()
        non_blank_mask = stripped_col != ""
        total_non_blank = non_blank_mask.sum()
        unique_count = stripped_col[non_blank_mask].nunique()
        
        matched = (df["Case__c_Flag"] == "Y").sum()
        unmatched = (df["Case__c_Flag"] == "N").sum()
        
        unmatched_stripped = stripped_col[df["Case__c_Flag"] == "N"]
        unique_unmatched = unmatched_stripped.nunique()
        unmatched_values = unmatched_stripped.unique()
        
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
    
    # === AUDIT CASE SURVEY FIELD ===
    print("\n" + "="*70)
    print("🔍 AUDITING CASE SURVEY FIELD (Case_Survey__c)")
    print("="*70)
    
    if "Case_Survey__c" in df.columns:
        df = create_lkp_and_flag(df, "Case_Survey__c", case_survey_lookup_dict)
        
        stripped_col = df["Case_Survey__c"].astype(str).str.strip()
        non_blank_mask = stripped_col != ""
        total_non_blank = non_blank_mask.sum()
        unique_count = stripped_col[non_blank_mask].nunique()
        
        matched = (df["Case_Survey__c_Flag"] == "Y").sum()
        unmatched = (df["Case_Survey__c_Flag"] == "N").sum()
        
        unmatched_stripped = stripped_col[df["Case_Survey__c_Flag"] == "N"]
        unique_unmatched = unmatched_stripped.nunique()
        unmatched_values = unmatched_stripped.unique()
        
        audit_summary["Case_Survey__c"] = {
            "total_non_blank": total_non_blank,
            "unique_count": unique_count,
            "matched": matched,
            "unmatched": unmatched,
            "unique_unmatched": unique_unmatched
        }
        
        print(f"\n• Case_Survey__c:")
        print(f"  Total non-blank records: {total_non_blank:,}")
        print(f"  Unique values: {unique_count:,}")
        print(f"  Matched: {matched:,}")
        print(f"  Unmatched: {unmatched:,} (Unique: {unique_unmatched:,})")
    else:
        print("\n• Case_Survey__c: Column not found in source")
    
    # === BUILD DETAIL REPORT ===
    print("\n" + "="*70)
    print("📝 GENERATING REPORTS")
    print("="*70)
    
    detail_fields = ["Legacy_SF_Record_ID__c"]
    
    for col in user_fields:
        if col in df.columns:
            detail_fields.extend([col, f"{col}_Lkp", f"{col}_Flag"])
    
    if "Case__c" in df.columns:
        detail_fields.extend(["Case__c", "Case__c_Lkp", "Case__c_Flag"])
    
    if "Case_Survey__c" in df.columns:
        detail_fields.extend(["Case_Survey__c", "Case_Survey__c_Lkp", "Case_Survey__c_Flag"])
    
    existing_fields = [f for f in detail_fields if f in df.columns]
    detail_df = df[existing_fields]
    
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    detail_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_DetailReport.csv")
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
        
        if field in user_fields:
            row["Default_ID"] = stats.get("default_id", "N/A")
        else:
            row["Default_ID"] = "N/A (blank if unmapped)"
        
        summary_rows.append(row)
    
    summary_df = pd.DataFrame(summary_rows)
    summary_csv = os.path.join(OUTPUT_DIR, f"{source_basename}_SummaryReport.csv")
    summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Summary report → {summary_csv}")
    
    # === GENERATE UNMAPPED USER FILES ===
    if unmapped_user_data:
        print("\n📝 Generating unmapped user reports...")
        
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
                print(f"   ✅ {col} unique unmapped → {unique_csv} ({len(unique_rows):,} IDs)")
    
    # === FINAL SUMMARY ===
    print("\n" + "="*70)
    print("📋 AUDIT SUMMARY")
    print("="*70)
    print(f"\n📊 TOTAL RECORDS IN SOURCE: {total_records:,}")
    print(f"📋 TOTAL COLUMNS: {len(df.columns)}")
    print("\n" + "-"*70)
    
    for field, stats in audit_summary.items():
        unmatched = stats.get("unmatched", 0)
        unique_unmatched = stats.get("unique_unmatched", 0)
        
        if unmatched > 0:
            print(f"⚠️ {field}:")
            print(f"   Unmatched: {unmatched:,} records (Unique: {unique_unmatched:,})")
            
            if field in user_fields:
                default_id = stats.get("default_id", "N/A")
                print(f"   → Will be replaced with DEFAULT: {default_id}")
            else:
                print(f"   → Will remain BLANK in output")
        else:
            print(f"✅ {field}: All matched")
    
    print("\n" + "="*70)
    print("✅ AUDIT COMPLETED!")
    print("="*70)


if __name__ == "__main__":
    main()

