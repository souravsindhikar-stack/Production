import os
import pandas as pd
import hashlib
import re
import shutil

# ========= USER INPUTS =========
# Source file containing Case Survey Junction data
SOURCE_FILE = r"D:\Production\Source\CaseSurveyJunction_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
CASE_LOOKUP_FILE = r"D:\Production\Lkp Files\Case_Lkp.csv"
CASE_SURVEY_LOOKUP_FILE = r"D:\Production\Lkp Files\CaseSurvey_Lkp.csv"

# Output directories
OUTPUT_DIR = r"D:\Production\Output"
SPLIT_DIR = r"D:\Production\Output\Parts"

# ========= CONSTANTS =========
# Default IDs for user fields
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# Split settings
MAX_ROWS = 100000
MAX_SIZE_MB = 200

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


# ==================== CLEANING FUNCTIONS ====================

ISO_TZ_REGEX = re.compile(r'^\s*\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})?\s*$')

DATE_PATTERNS = [
    re.compile(r'^\s*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$'),
    re.compile(r'^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$'),
    re.compile(r'^\s*\d{1,2}[-.]\w{3}[-.]\d{2,4}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?\s*$', re.I)
]

TIME_REGEX = re.compile(r'(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?')


def _clamp_2d(val_str: str, maxv: int) -> str:
    try:
        v = int(val_str)
    except Exception:
        v = 0
    v = max(0, min(maxv, v))
    return f"{v:02d}"


def _fix_invalid_time(text: str) -> str:
    def _repl(m: re.Match) -> str:
        h = _clamp_2d(m.group(1), 23)
        mi = _clamp_2d(m.group(2), 59)
        s = m.group(3)
        if s is not None:
            s = _clamp_2d(s, 59)
            return f"{h}:{mi}:{s}"
        else:
            return f"{h}:{mi}"
    return TIME_REGEX.sub(_repl, str(text), count=1)


def _looks_like_date(s: str) -> bool:
    if ISO_TZ_REGEX.match(s):
        return True
    return any(pat.match(s) for pat in DATE_PATTERNS)


def normalize_cell(val):
    """Normalize only if it looks like a valid date/time string."""
    if isinstance(val, (list, pd.Series)):
        if len(val) == 0:
            return val
        val = val.iloc[0] if isinstance(val, pd.Series) else val[0]

    if pd.isna(val):
        return val

    s = str(val).strip()
    if s == "":
        return s
    if not _looks_like_date(s):
        return val
    if ISO_TZ_REGEX.match(s):
        return s

    s_fixed = _fix_invalid_time(s)

    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = pd.to_datetime(s_fixed, format=fmt, errors='raise')
            has_time = any(x in fmt for x in ["%H", "%M", "%S"])
            return dt.strftime("%Y-%m-%d %H:%M:%S") if has_time else dt.strftime("%Y-%m-%d")
        except:
            continue

    dt = pd.to_datetime(s_fixed, errors="coerce")
    if pd.isna(dt):
        return val
    has_time = (":" in s_fixed) or (dt.hour or dt.minute or dt.second)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if has_time else dt.strftime("%Y-%m-%d")


def split_csv(df, base_name, output_dir, max_rows=100000, max_size_mb=200):
    """Split DataFrame into smaller CSV files"""
    rows = len(df)
    part = 1
    start = 0
    max_allowed_size = max_size_mb * 0.99

    print(f"\n📂 Splitting file: {rows:,} rows, ~{df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB in memory")

    while start < rows:
        end = min(start + max_rows, rows)
        chunk = df.iloc[start:end]

        output_file = os.path.join(output_dir, f"{base_name}_part{part}.csv")
        chunk.to_csv(output_file, index=False, encoding="utf-8")

        size_mb = os.path.getsize(output_file) / (1024 * 1024)

        while size_mb > max_allowed_size and (end - start) > 1:
            overshoot_factor = size_mb / max_allowed_size
            new_chunk_size = int((end - start) / overshoot_factor)
            if new_chunk_size < 1:
                new_chunk_size = 1

            end = start + new_chunk_size
            chunk = df.iloc[start:end]
            chunk.to_csv(output_file, index=False, encoding="utf-8")
            size_mb = os.path.getsize(output_file) / (1024 * 1024)

        print(f"   ✅ {os.path.basename(output_file)} ({size_mb:.2f} MB, {len(chunk):,} rows)")

        start = end
        part += 1


def hash_row(row):
    """Create hash for data validation"""
    row_str = "|".join(str(v) if pd.notna(v) else "NULL" for v in row)
    return hashlib.md5(row_str.encode()).hexdigest()


def validate_data(cleaned_file, split_dir):
    """Validate split files match the cleaned file"""
    print("\n🔍 Validating data integrity...")
    
    df_original = pd.read_csv(cleaned_file, dtype=str, encoding="utf-8")
    orig_rows = len(df_original)
    orig_hashes = set(df_original.apply(hash_row, axis=1))

    split_files = [f for f in os.listdir(split_dir) if f.endswith(".csv")]
    combined_rows = 0
    combined_hashes = set()
    
    for f in split_files:
        path = os.path.join(split_dir, f)
        df_split = pd.read_csv(path, dtype=str, encoding="utf-8")
        combined_rows += len(df_split)
        combined_hashes.update(df_split.apply(hash_row, axis=1))

    if orig_rows == combined_rows:
        print(f"   ✅ Row count matches: {orig_rows:,} rows")
    else:
        print(f"   ❌ Row count mismatch: Original={orig_rows:,}, Split={combined_rows:,}")
    
    if orig_hashes == combined_hashes:
        print("   ✅ Data integrity passed")
    else:
        print("   ❌ Data mismatch detected!")


# ==================== LOOKUP FUNCTIONS ====================

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


# ==================== MAIN FUNCTION ====================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    print("="*70)
    print("🚀 CASE SURVEY JUNCTION - MAPPING + CLEANING")
    print("="*70)
    
    # === LOAD LOOKUP FILES ===
    print("\n📖 Loading lookup files...")
    
    print("   • User lookup...")
    user_lookup_dict = load_user_lookup(USER_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(user_lookup_dict)} user mappings")
    
    print("   • Case lookup...")
    case_lookup_dict = load_simple_lookup(CASE_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_lookup_dict)} case mappings")
    
    print("   • Case Survey lookup...")
    case_survey_lookup_dict = load_simple_lookup(CASE_SURVEY_LOOKUP_FILE)
    print(f"     ✅ Loaded {len(case_survey_lookup_dict)} case survey mappings")
    
    # === PREPARE OUTPUT FILES ===
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    mapped_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")
    cleaned_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_cleaned.csv")
    
    if os.path.exists(mapped_output_file):
        os.remove(mapped_output_file)
    
    # === TRACK UNMAPPED RECORDS ===
    unmapped_data = {
        "Case__c": [],
        "Case_Survey__c": [],
    }
    
    # === PROCESS SOURCE FILE ===
    print("\n🔄 Processing source file...")
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        
        # Store original values for tracking
        original_values = {}
        for col in unmapped_data.keys():
            if col in chunk.columns:
                original_values[col] = chunk[col].astype(str).str.strip().copy()
        
        # === STANDARD USER LOOKUP (CreatedById, LastModifiedById) ===
        user_fields = ["CreatedById", "LastModifiedById"]
        
        for col in user_fields:
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(
                    lambda val: user_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
                )
                
                # Apply default for blank/unmapped values
                mask = chunk[col].astype(str).str.strip() == ""
                chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID
        
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
        
        # === CASE_SURVEY__C LOOKUP ===
        if "Case_Survey__c" in chunk.columns:
            original = original_values.get("Case_Survey__c", pd.Series([""] * len(chunk)))
            
            chunk["Case_Survey__c"] = chunk["Case_Survey__c"].apply(
                lambda val: case_survey_lookup_dict.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped = chunk["Case_Survey__c"].astype(str).str.strip()
            
            # Track unmapped
            unmapped_mask = (original != "") & (mapped == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                unmapped_rows["Case_Survey__c"] = original[unmapped_mask].values
                if "Id" in chunk.columns:
                    unmapped_data["Case_Survey__c"].append(unmapped_rows[["Id", "Case_Survey__c"]])
        
        # Write to mapped output
        chunk.to_csv(
            mapped_output_file,
            index=False,
            mode="a" if header_written else "w",
            header=not header_written,
            encoding="utf-8-sig",
        )
        header_written = True
        total_rows += len(chunk)
        
        print(f"   ✅ Chunk {chunk_idx}: {len(chunk):,} rows processed")
    
    print(f"\n📊 Total rows mapped: {total_rows:,}")
    
    # === WRITE UNMAPPED FILES ===
    print("\n📝 Writing unmapped reports...")
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
            print(f"   ✅ {col}: All mapped")
    
    # === APPLY CLEANING ===
    print("\n" + "="*70)
    print("🧹 APPLYING CLEANING")
    print("="*70)
    
    print("\n📖 Loading mapped file for cleaning...")
    df = pd.read_csv(mapped_output_file, dtype=str, encoding="utf-8-sig")
    df = df.fillna("")
    
    # Remove completely empty columns
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~(df.apply(lambda x: x.astype(str).str.strip().eq('').all()))]
    print(f"   ✅ Removed empty columns. Remaining: {len(df.columns)} columns")
    
    # Normalize date-like fields
    print("   🔄 Normalizing date fields...")
    for col in df.columns:
        df[col] = df[col].apply(normalize_cell)
    print("   ✅ Date normalization complete")
    
    # Rename "Id" column to "Legacy_SF_Record_ID__c" if present
    if "Id" in df.columns:
        df.rename(columns={"Id": "Legacy_SF_Record_ID__c"}, inplace=True)
        print("   ✅ Renamed 'Id' to 'Legacy_SF_Record_ID__c'")
    else:
        print("   ⚠️ 'Id' column not found")
    
    # Save cleaned file
    df.to_csv(cleaned_output_file, index=False, encoding="utf-8")
    print(f"\n✅ Cleaned file saved: {cleaned_output_file}")
    
    cleaned_size_mb = os.path.getsize(cleaned_output_file) / (1024 * 1024)
    print(f"   📏 File size: {cleaned_size_mb:.2f} MB")
    
    # === SPLIT FILES ===
    print("\n" + "="*70)
    print("📂 SPLITTING FILES")
    print("="*70)
    
    if os.path.exists(SPLIT_DIR):
        shutil.rmtree(SPLIT_DIR)
    os.makedirs(SPLIT_DIR, exist_ok=True)
    
    base_name = os.path.splitext(os.path.basename(cleaned_output_file))[0]
    split_csv(df, base_name, SPLIT_DIR, max_rows=MAX_ROWS, max_size_mb=MAX_SIZE_MB)
    
    # === VALIDATE ===
    print("\n" + "="*70)
    print("🔍 VALIDATION")
    print("="*70)
    
    validate_data(cleaned_output_file, SPLIT_DIR)
    
    # === FINAL SUMMARY ===
    print("\n" + "="*70)
    print("✅ CASE SURVEY JUNCTION - MAPPING + CLEANING COMPLETED!")
    print("="*70)
    print(f"\n📊 Total rows processed: {total_rows:,}")
    print(f"📝 Mapped output: {mapped_output_file}")
    print(f"📝 Cleaned output: {cleaned_output_file}")
    print(f"📂 Split files: {SPLIT_DIR}")
    
    total_unmapped = sum(unmapped_counts.values())
    if total_unmapped > 0:
        print(f"\n⚠️ Total unmapped: {total_unmapped:,}")
        for col, count in unmapped_counts.items():
            if count > 0:
                print(f"   - {col}: {count:,}")
    else:
        print("\n✅ All lookups mapped successfully!")


if __name__ == "__main__":
    main()

