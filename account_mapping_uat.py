import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Account data
SOURCE_FILE = r"D:\UATMigration\Source\Account_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\UATMigration\Lookup Files\User_Lkp.csv"
ACCOUNT_LOOKUP_FILE = r"D:\UATMigration\Lookup Files\Account_Lkp.xlsx"
CONTACT_LOOKUP_FILE = r"D:\UATMigration\Lookup Files\merged_contactId_Lkp.csv"

# Output directory
OUTPUT_DIR = r"D:\UATMigration\Output"

# ========= CONSTANTS =========
DEFAULT_OWNER_ID = "005Wr00000ECxAjIAL"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# RecordTypeId mappings
RECORDTYPE_MAPPINGS = {
    "0120g000000URc1AAG": "012Wr000001eRx5IAE",
    "012700000001aqvAAA": "012Wr000001eRx4IAE",
}

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a simple key-value lookup file (Account, Contact)"""
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


def load_user_lookup(path):
    """Load user lookup file and return mapping function (UAT multi-step logic)"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"User lookup file not found: {path}")

    df = pd.read_csv(path, dtype=str).fillna("")

    # Strip whitespace from all columns
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    # Build mapping dictionaries
    # Step 1: digital_prod_Id -> digital_Global_ID__c
    dict_digital_to_global = {
        str(k).lower(): str(v)
        for k, v in zip(df["digital_prod_Id"], df["digital_Global_ID__c"])
        if str(k).strip()
    }

    # Step 2: merge_Global_ID__c -> merge_Id
    dict_global_to_merge = {
        str(k).lower(): str(v)
        for k, v in zip(df["merge_Global_ID__c"], df["merge_Id"])
        if str(k).strip()
    }

    # Fallback: (email, name) -> merge_Id
    dict_email_name_to_merge = {
        (str(row["digital_Email"]).lower(), str(row["digital_Name"]).lower()): str(row["merge_Id"])
        for _, row in df.iterrows()
        if str(row["digital_Email"]).strip() and str(row["digital_Name"]).strip()
    }

    dict_digital_to_email_name = {
        str(row["digital_prod_Id"]).lower(): (
            str(row["digital_Email"]).lower(),
            str(row["digital_Name"]).lower(),
        )
        for _, row in df.iterrows()
        if str(row["digital_prod_Id"]).strip()
    }

    def map_user_id(digital_prod_id):
        """Map user ID using multi-step logic"""
        if not digital_prod_id or str(digital_prod_id).strip() == "":
            return ""

        digital_prod_id_lc = str(digital_prod_id).strip().lower()

        # Step 1: digital_prod_Id -> digital_Global_ID__c
        digital_global_id = dict_digital_to_global.get(digital_prod_id_lc, "").lower()

        # Step 2: digital_Global_ID__c -> merge_Id
        if digital_global_id and digital_global_id in dict_global_to_merge:
            return dict_global_to_merge[digital_global_id]

        # Fallback: Try email/name match
        email_name = dict_digital_to_email_name.get(digital_prod_id_lc)
        if email_name:
            email_name_lc = (email_name[0].lower(), email_name[1].lower())
            mapped = dict_email_name_to_merge.get(email_name_lc, "")
            if mapped:
                return mapped

        return ""

    return map_user_id


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    print("=" * 80)
    print("ACCOUNT MAPPING SCRIPT (UAT)")
    print("=" * 80)

    print("\n📖 Loading lookup files...")
    map_user_id = load_user_lookup(USER_LOOKUP_FILE)
    print("   ✅ User lookup loaded (UAT multi-step logic)")

    dict_account = load_simple_lookup(ACCOUNT_LOOKUP_FILE)
    print(f"   ✅ Account lookup: {len(dict_account)} mappings")

    dict_contact = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"   ✅ Contact lookup: {len(dict_contact)} mappings")

    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")

    # Remove existing output file
    if os.path.exists(main_output_file):
        os.remove(main_output_file)

    # Track unmapped records per column
    unmapped_data = {
        "ParentId": [],
        "Ops_Agent__c": [],  # Will be renamed to SMR__c
        "Expediter__c": [],
        "Primary_Supplier_Contact__c": [],
    }

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("\n🔄 Processing source file in chunks...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")

        # Store original values for unmapped tracking
        original_values = {}
        for col in unmapped_data.keys():
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()

        # === STEP 1: DELETE EXISTING SMR__c COLUMN (if exists) ===
        if "SMR__c" in chunk.columns:
            chunk.drop(columns=["SMR__c"], inplace=True)

        # === STEP 2: STANDARD USER LOOKUP (OwnerId, CreatedById, LastModifiedById) ===
        # Logic: Always apply default if blank or unmapped (NO unmapped files for these)
        standard_user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]

        for col in standard_user_fields:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(map_user_id)

                # Apply defaults for blank/unmapped values
                if col == "OwnerId":
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_OWNER_ID
                else:  # CreatedById, LastModifiedById
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID

        # === STEP 3: CUSTOM USER LOOKUP (Ops_Agent__c, Expediter__c) ===
        # Logic: Blank stays blank, non-blank unmapped → separate file (keep blank in output)
        custom_user_fields = ["Ops_Agent__c", "Expediter__c"]

        for col in custom_user_fields:
            if col in chunk.columns:
                original = original_values.get(col, pd.Series([""] * len(chunk)))

                chunk[col] = chunk[col].apply(map_user_id)
                mapped = chunk[col].astype(str).str.strip()

                # Track unmapped (source had value but mapping failed)
                unmapped_mask = (original != "") & (mapped == "")
                if unmapped_mask.any():
                    unmapped_rows = pd.DataFrame({
                        "Id": chunk.loc[unmapped_mask, "Id"].values if "Id" in chunk.columns else [""] * unmapped_mask.sum(),
                        col: original[unmapped_mask].values,
                    })
                    unmapped_data[col].append(unmapped_rows)

        # === STEP 4: RENAME Ops_Agent__c TO SMR__c ===
        if "Ops_Agent__c" in chunk.columns:
            chunk.rename(columns={"Ops_Agent__c": "SMR__c"}, inplace=True)

        # === STEP 5: ACCOUNT LOOKUP (ParentId) ===
        if "ParentId" in chunk.columns:
            original = original_values.get("ParentId", pd.Series([""] * len(chunk)))

            chunk["ParentId"] = chunk["ParentId"].apply(
                lambda val: dict_account.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            mapped = chunk["ParentId"].astype(str).str.strip()

            # Track unmapped
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = pd.DataFrame({
                    "Id": chunk.loc[unmapped_mask, "Id"].values if "Id" in chunk.columns else [""] * unmapped_mask.sum(),
                    "ParentId": original[unmapped_mask].values,
                })
                unmapped_data["ParentId"].append(unmapped_rows)

        # === STEP 6: CONTACT LOOKUP (Primary_Supplier_Contact__c) ===
        if "Primary_Supplier_Contact__c" in chunk.columns:
            original = original_values.get("Primary_Supplier_Contact__c", pd.Series([""] * len(chunk)))

            chunk["Primary_Supplier_Contact__c"] = chunk["Primary_Supplier_Contact__c"].apply(
                lambda val: dict_contact.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            mapped = chunk["Primary_Supplier_Contact__c"].astype(str).str.strip()

            # Track unmapped
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = pd.DataFrame({
                    "Id": chunk.loc[unmapped_mask, "Id"].values if "Id" in chunk.columns else [""] * unmapped_mask.sum(),
                    "Primary_Supplier_Contact__c": original[unmapped_mask].values,
                })
                unmapped_data["Primary_Supplier_Contact__c"].append(unmapped_rows)

        # === STEP 7: RECORDTYPEID REPLACEMENT ===
        if "RecordTypeId" in chunk.columns:
            chunk["RecordTypeId"] = chunk["RecordTypeId"].apply(
                lambda val: RECORDTYPE_MAPPINGS.get(str(val).strip(), str(val).strip())
            )

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

    # === WRITE UNMAPPED FILES ===
    print("\n📝 Writing unmapped reports...")
    unmapped_counts = {}

    for col, data_list in unmapped_data.items():
        if data_list:
            unmapped_df = pd.concat(data_list, ignore_index=True)
            # Use original column name for file (Ops_Agent__c instead of SMR__c)
            unmapped_file = os.path.join(OUTPUT_DIR, f"{col}_unmapped.csv")
            unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
            unmapped_counts[col] = len(unmapped_df)
            print(f"   ⚠️ {col}: {len(unmapped_df)} unmapped → {unmapped_file}")
        else:
            unmapped_counts[col] = 0
            print(f"   ✅ {col}: All records mapped successfully")

    # === SUMMARY ===
    print("\n" + "=" * 80)
    print("MAPPING SUMMARY")
    print("=" * 80)
    print(f"\n📊 Total rows processed: {total_rows:,}")
    print(f"📝 Main output: {main_output_file}")

    if os.path.exists(main_output_file):
        main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
        print(f"📏 Output file size: {main_size_mb:.2f} MB")

    print("\n--- Standard User Fields ---")
    print(f"   OwnerId: Default = {DEFAULT_OWNER_ID}")
    print(f"   CreatedById: Default = {DEFAULT_CREATEDBY_LASTMODIFIED_ID}")
    print(f"   LastModifiedById: Default = {DEFAULT_CREATEDBY_LASTMODIFIED_ID}")

    print("\n--- Custom User Fields ---")
    print(f"   Ops_Agent__c → SMR__c (renamed in output)")
    print(f"   Expediter__c")
    print("   (Blank stays blank, unmapped tracked in separate files)")

    print("\n--- RecordTypeId Replacements ---")
    for old_val, new_val in RECORDTYPE_MAPPINGS.items():
        print(f"   {old_val} → {new_val}")

    total_unmapped = sum(unmapped_counts.values())
    print(f"\n⚠️ Total unmapped records: {total_unmapped}")

    if total_unmapped > 0:
        print("\nUnmapped breakdown:")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count}")

    print("\n" + "=" * 80)
    print("✅ MAPPING COMPLETED!")
    print("=" * 80)


if __name__ == "__main__":
    main()

