import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Talkdesk Activity Case Relation data
SOURCE_FILE = r"D:\Production\Talkdesk_Activity_Case_Relation\SOURCE_FILE.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\All_Case_Destination_Org_v1_3035227.csv"
TALKDESK_ACTIVITY_LOOKUP_FILE = r"D:\Production\Lkp Files\TALKDESK_ACTIVITY_LOOKUP_FILE.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Talkdesk_Activity_Case_Relation\Mapped"

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

    print("📖 Loading lookup files...")
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Case lookup...")
    dict_case = load_simple_lookup(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(dict_case)} case mappings")
    
    print("   • Talkdesk Activity lookup...")
    dict_talkdesk_activity = load_simple_lookup(TALKDESK_ACTIVITY_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(dict_talkdesk_activity)} talkdesk activity mappings")

    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")

    # Remove existing output files
    if os.path.exists(main_output_file):
        os.remove(main_output_file)

    # Track unmapped records per column
    unmapped_case = []
    unmapped_talkdesk_activity = []

    # Track RFPD blanked records for Case
    rfpd_blanked_case = []

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("🔄 Processing source file in chunks...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")

        # Store original values for unmapped tracking
        original_case = chunk.get("talkdesk__Case__c", pd.Series([""] * len(chunk))).astype(str).str.strip().copy()
        original_talkdesk_activity = chunk.get("talkdesk__Talkdesk_Activity__c", pd.Series([""] * len(chunk))).astype(str).str.strip().copy()

        # === CASE RECORDTYPE BLANKING (RFPD) ===
        if "talkdesk__Case__c" in chunk.columns and "talkdesk__Case__r.recordtype.Name" in chunk.columns:
            recordtype_col = "talkdesk__Case__r.recordtype.Name"

            # RFPD blanking
            rfpd_mask = chunk[recordtype_col].astype(str).str.strip().str.upper() == "RFPD"
            if rfpd_mask.any():
                rfpd_rows = chunk[rfpd_mask][["Id", "talkdesk__Case__c"]].copy()
                rfpd_rows.columns = ["Id", "talkdesk__Case__c_original"]
                rfpd_blanked_case.append(rfpd_rows)
                chunk.loc[rfpd_mask, "talkdesk__Case__c"] = ""
                # Update original_case to reflect blanking (so we don't track as unmapped)
                original_case = chunk["talkdesk__Case__c"].astype(str).str.strip().copy()

        # === STANDARD USER LOOKUP (CreatedById, LastModifiedById) ===
        user_fields = ["CreatedById", "LastModifiedById"]

        for col in user_fields:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                # Always set default if blank/unmapped
                mask = chunk[col].astype(str).str.strip() == ""
                chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID

        # === CASE LOOKUP ===
        if "talkdesk__Case__c" in chunk.columns:
            chunk["talkdesk__Case__c"] = chunk["talkdesk__Case__c"].apply(
                lambda val: dict_case.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            mapped_case = chunk["talkdesk__Case__c"].astype(str).str.strip()

            # Track unmapped: source had value but no match found
            unmapped_mask = (original_case != "") & (mapped_case == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask][["Id"]].copy()
                unmapped_rows["talkdesk__Case__c_original"] = original_case[unmapped_mask].values
                unmapped_case.append(unmapped_rows)

        # === TALKDESK ACTIVITY LOOKUP ===
        if "talkdesk__Talkdesk_Activity__c" in chunk.columns:
            chunk["talkdesk__Talkdesk_Activity__c"] = chunk["talkdesk__Talkdesk_Activity__c"].apply(
                lambda val: dict_talkdesk_activity.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            mapped_talkdesk_activity = chunk["talkdesk__Talkdesk_Activity__c"].astype(str).str.strip()

            # Track unmapped: source had value but no match found
            unmapped_mask = (original_talkdesk_activity != "") & (mapped_talkdesk_activity == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask][["Id"]].copy()
                unmapped_rows["talkdesk__Talkdesk_Activity__c_original"] = original_talkdesk_activity[unmapped_mask].values
                unmapped_talkdesk_activity.append(unmapped_rows)

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

    # === WRITE RFPD BLANKED FILES ===
    print("\n📝 Writing RFPD blanked reports...")

    if rfpd_blanked_case:
        rfpd_df = pd.concat(rfpd_blanked_case, ignore_index=True)
        rfpd_file = os.path.join(OUTPUT_DIR, "talkdesk__Case__c_RFPD_blanked.csv")
        rfpd_df.to_csv(rfpd_file, index=False, encoding="utf-8-sig")
        print(f"   📄 RFPD Case blanked: {len(rfpd_df)} records → {rfpd_file}")
    else:
        print("   ✅ No RFPD Case records to blank")

    # === WRITE UNMAPPED FILES ===
    print("\n📝 Writing unmapped reports...")

    unmapped_counts = {}

    if unmapped_case:
        unmapped_df = pd.concat(unmapped_case, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "talkdesk__Case__c_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        unmapped_counts["talkdesk__Case__c"] = len(unmapped_df)
        print(f"   ⚠️ talkdesk__Case__c: {len(unmapped_df)} unmapped → {unmapped_file}")
    else:
        unmapped_counts["talkdesk__Case__c"] = 0

    if unmapped_talkdesk_activity:
        unmapped_df = pd.concat(unmapped_talkdesk_activity, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "talkdesk__Talkdesk_Activity__c_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        unmapped_counts["talkdesk__Talkdesk_Activity__c"] = len(unmapped_df)
        print(f"   ⚠️ talkdesk__Talkdesk_Activity__c: {len(unmapped_df)} unmapped → {unmapped_file}")
    else:
        unmapped_counts["talkdesk__Talkdesk_Activity__c"] = 0

    # === SUMMARY ===
    print("\n" + "=" * 60)
    print("✅ MAPPING COMPLETED!")
    print("=" * 60)
    print(f"📊 Total rows processed: {total_rows}")
    print(f"📝 Main output: {main_output_file}")

    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")

    # RFPD blanked summary
    rfpd_count = len(pd.concat(rfpd_blanked_case, ignore_index=True)) if rfpd_blanked_case else 0
    print(f"\n📋 RFPD Case blanked: {rfpd_count}")

    # Unmapped summary
    total_unmapped = sum(unmapped_counts.values())
    print(f"\n⚠️ Total unmapped records: {total_unmapped}")
    if total_unmapped > 0:
        print("Unmapped breakdown:")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count}")


if __name__ == "__main__":
    main()

