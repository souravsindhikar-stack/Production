import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Case data with Id and ParentId
SOURCE_FILE = r"D:\Production\Case\Case_ParentId_Source.csv"

# Lookup file for Case
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\All_Case_Destination_Org_v1_3035227.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Case\Mapped"

# ========= CONSTANTS =========
CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_case_lookup(path):
    """Load case lookup file and return dictionary mapping Legacy_SF_Record_ID__c -> Id"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Case lookup file not found: {path}")

    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    df = df.fillna("")

    if "Legacy_SF_Record_ID__c" not in df.columns or "Id" not in df.columns:
        raise ValueError(f"Case lookup file must contain 'Legacy_SF_Record_ID__c' and 'Id' columns")

    # Strip whitespace
    df["Legacy_SF_Record_ID__c"] = df["Legacy_SF_Record_ID__c"].astype(str).str.strip()
    df["Id"] = df["Id"].astype(str).str.strip()

    # Build lowercase key dictionary
    return {
        str(k).strip().lower(): str(v).strip()
        for k, v in zip(df["Legacy_SF_Record_ID__c"], df["Id"])
        if str(k).strip()
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    print("=" * 80)
    print("CASE PARENTID MAPPING SCRIPT")
    print("=" * 80)

    # Load case lookup
    print("\nLoading case lookup file...")
    case_lookup_dict = load_case_lookup(CASE_LOOKUP_FILE)
    print(f"   ✅ Loaded {len(case_lookup_dict)} case mappings")

    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")

    # Remove existing output file
    if os.path.exists(main_output_file):
        os.remove(main_output_file)

    # Track unmapped records
    unmapped_id = []
    unmapped_parentid = []

    # Track stats
    stats = {
        "Id": {"total": 0, "nonblank": 0, "matched": 0, "unmatched": 0},
        "ParentId": {"total": 0, "nonblank": 0, "matched": 0, "unmatched": 0},
    }

    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0

    print("\nProcessing source file...")

    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")

        # Store original values for tracking
        original_id = chunk["Id"].astype(str).str.strip().copy()
        original_parentid = chunk.get("ParentId", pd.Series([""] * len(chunk))).astype(str).str.strip().copy()

        # === STEP 1: Create "destination org case Id" by mapping original Id ===
        destination_case_id = original_id.apply(
            lambda val: case_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
        )

        # === STEP 2: Map ParentId using case lookup ===
        mapped_parentid = original_parentid.apply(
            lambda val: case_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
        )

        # === STEP 3: Build output dataframe with 3 columns ===
        output_df = pd.DataFrame({
            "destination org case Id": destination_case_id,
            "Legacy_SF_Record_ID__c": original_id,
            "ParentId": mapped_parentid,
        })

        # === TRACK STATS FOR Id ===
        stats["Id"]["total"] += len(chunk)
        nonblank_id_mask = original_id != ""
        stats["Id"]["nonblank"] += nonblank_id_mask.sum()
        matched_id_mask = destination_case_id != ""
        stats["Id"]["matched"] += (nonblank_id_mask & matched_id_mask).sum()
        unmatched_id_mask = nonblank_id_mask & ~matched_id_mask
        stats["Id"]["unmatched"] += unmatched_id_mask.sum()

        # Track unmapped Id
        if unmatched_id_mask.any():
            unmapped_rows = pd.DataFrame({
                "Legacy_SF_Record_ID__c": original_id[unmatched_id_mask].values
            })
            unmapped_id.append(unmapped_rows)

        # === TRACK STATS FOR ParentId ===
        stats["ParentId"]["total"] += len(chunk)
        nonblank_parentid_mask = original_parentid != ""
        stats["ParentId"]["nonblank"] += nonblank_parentid_mask.sum()
        matched_parentid_mask = mapped_parentid != ""
        stats["ParentId"]["matched"] += (nonblank_parentid_mask & matched_parentid_mask).sum()
        unmatched_parentid_mask = nonblank_parentid_mask & ~matched_parentid_mask
        stats["ParentId"]["unmatched"] += unmatched_parentid_mask.sum()

        # Track unmapped ParentId
        if unmatched_parentid_mask.any():
            unmapped_rows = pd.DataFrame({
                "Legacy_SF_Record_ID__c": original_id[unmatched_parentid_mask].values,
                "ParentId": original_parentid[unmatched_parentid_mask].values,
            })
            unmapped_parentid.append(unmapped_rows)

        # Write to output file
        output_df.to_csv(
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

    if unmapped_id:
        unmapped_df = pd.concat(unmapped_id, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "Id_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        print(f"   ⚠️ Id (destination org case Id): {len(unmapped_df)} unmapped → {unmapped_file}")
    else:
        print("   ✅ Id: All records mapped successfully")

    if unmapped_parentid:
        unmapped_df = pd.concat(unmapped_parentid, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "ParentId_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        print(f"   ⚠️ ParentId: {len(unmapped_df)} unmapped → {unmapped_file}")
    else:
        print("   ✅ ParentId: All records mapped successfully")

    # === SUMMARY ===
    print("\n" + "=" * 80)
    print("MAPPING SUMMARY")
    print("=" * 80)
    print(f"\nTotal records processed: {total_rows:,}")

    print("\n--- Id → destination org case Id ---")
    s = stats["Id"]
    print(f"   Total: {s['total']:,}")
    print(f"   Non-blank: {s['nonblank']:,}")
    print(f"   Matched: {s['matched']:,}")
    print(f"   Unmatched: {s['unmatched']:,}")

    print("\n--- ParentId ---")
    s = stats["ParentId"]
    print(f"   Total: {s['total']:,}")
    print(f"   Non-blank: {s['nonblank']:,}")
    print(f"   Matched: {s['matched']:,}")
    print(f"   Unmatched: {s['unmatched']:,}")

    print("\n" + "=" * 80)
    print("OUTPUT FILE COLUMNS:")
    print("=" * 80)
    print("   1. destination org case Id  → New mapped ID from case lookup")
    print("   2. Legacy_SF_Record_ID__c   → Original Id renamed (old IDs)")
    print("   3. ParentId                 → New mapped ParentId from case lookup")

    print("\n" + "=" * 80)
    print("✅ MAPPING COMPLETED!")
    print("=" * 80)
    print(f"\nMain output: {main_output_file}")

    if os.path.exists(main_output_file):
        main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
        print(f"Output file size: {main_size_mb:.2f} MB")


if __name__ == "__main__":
    main()

