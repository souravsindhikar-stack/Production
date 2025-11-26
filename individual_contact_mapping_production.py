import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Individual data (already cleaned with user mapping & RFPD blanking done)
SOURCE_FILE = r"D:\Production\Source\Individual_Cleaned.csv"

# Lookup file
CONTACT_LOOKUP_FILE = r"D:\Production\Lkp Files\Contact_Lkp.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a simple key-value lookup file for Contact"""
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
    
    print("📖 Loading Contact lookup file...")
    dict_contact = load_simple_lookup(CONTACT_LOOKUP_FILE)
    print(f"   ✅ Loaded {len(dict_contact)} contact mappings")
    
    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")
    
    # Remove existing output files
    if os.path.exists(main_output_file):
        os.remove(main_output_file)
    
    # Track unmapped Contact_Origin__c records
    unmapped_contact_origin = []
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0
    
    print("🔄 Processing source file in chunks...")
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        
        # === CONTACT_ORIGIN__C - CONTACT LOOKUP ===
        if "Contact_Origin__c" in chunk.columns:
            # Store original values for unmapped tracking
            original_contact_origin = chunk["Contact_Origin__c"].astype(str).str.strip()
            
            # Apply contact lookup
            chunk["Contact_Origin__c"] = chunk["Contact_Origin__c"].apply(
                lambda val: dict_contact.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped_contact_origin = chunk["Contact_Origin__c"].astype(str).str.strip()
            
            # Track unmapped: original had value but mapping returned blank
            unmapped_mask = (original_contact_origin != "") & (mapped_contact_origin == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                # Store Legacy_SF_Record_ID__c and original Contact_Origin__c value
                if "Legacy_SF_Record_ID__c" in chunk.columns:
                    unmapped_rows["Contact_Origin__c"] = original_contact_origin[unmapped_mask].values
                    unmapped_contact_origin.append(unmapped_rows[["Legacy_SF_Record_ID__c", "Contact_Origin__c"]])
        
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
    
    # === WRITE UNMAPPED FILE ===
    if unmapped_contact_origin:
        print("\n📝 Writing unmapped Contact_Origin__c report...")
        unmapped_df = pd.concat(unmapped_contact_origin, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "Contact_Origin__c_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        print(f"   ⚠️ Contact_Origin__c: {len(unmapped_df)} unmapped → {unmapped_file}")
    
    # === SUMMARY ===
    print("\n" + "="*60)
    print("✅ INDIVIDUAL CONTACT MAPPING COMPLETED!")
    print("="*60)
    print(f"📊 Total rows processed: {total_rows}")
    print(f"📝 Main output: {main_output_file}")
    
    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")
    
    print("\n" + "-"*60)
    print("📋 MAPPING SUMMARY:")
    print("-"*60)
    
    if unmapped_contact_origin:
        total_unmapped = len(pd.concat(unmapped_contact_origin, ignore_index=True))
        print(f"⚠️ Contact_Origin__c: {total_unmapped} unmapped records")
        print(f"   (Blank values from RFPD blanking were preserved)")
    else:
        print("✅ Contact_Origin__c: All values mapped successfully!")
        print(f"   (Blank values from RFPD blanking were preserved)")


if __name__ == "__main__":
    main()

