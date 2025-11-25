import os
import pandas as pd

# ========= USER INPUTS =========
# Source file containing Messaging Session data
SOURCE_FILE = r"D:\Production\Source\MessagingSession_Source.csv"

# Lookup files
USER_LOOKUP_FILE = r"D:\Production\Lkp Files\DigitalVsComponent_User_Lkp_File.csv"
MESSAGING_ENDUSER_LOOKUP_FILE = r"D:\Production\Lkp Files\MessagingEndUser_Lkp.csv"

# Output directory
OUTPUT_DIR = r"D:\Production\Output"

# ========= CONSTANTS =========
# Default IDs for user fields
DEFAULT_OWNER_ID = "005Vq000008gEtBIAU"
DEFAULT_CREATEDBY_LASTMODIFIED_ID = "005A0000000rXeVIAU"

# Constant value for MessagingChannelId
MESSAGING_CHANNEL_ID = "0MjVq0000012ZabKAE"

CHUNK_SIZE = 50_000

# ========= END OF USER INPUTS =========


def load_user_lookup(path):
    """Load user lookup file and return simple mapping function"""
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
    
    def map_user_id(legacy_id):
        """Simple lookup: Legacy_SF_Record_ID__c -> Id"""
        if not legacy_id or str(legacy_id).strip() == "":
            return ""
        
        legacy_id_lc = str(legacy_id).strip().lower()
        return lookup_dict.get(legacy_id_lc, "")
    
    return map_user_id


def load_simple_lookup(path, key_col="Legacy_SF_Record_ID__c", value_col="Id"):
    """Load a simple key-value lookup file for MessagingEndUser"""
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
    
    print("📖 Loading user lookup file...")
    map_user_id = load_user_lookup(USER_LOOKUP_FILE)
    
    # Load MessagingEndUser lookup (only if file exists)
    dict_messaging_enduser = {}
    if os.path.exists(MESSAGING_ENDUSER_LOOKUP_FILE):
        print("📖 Loading MessagingEndUser lookup file...")
        dict_messaging_enduser = load_simple_lookup(MESSAGING_ENDUSER_LOOKUP_FILE)
    else:
        print(f"⚠️  MessagingEndUser lookup file not found: {MESSAGING_ENDUSER_LOOKUP_FILE}")
        print("   Skipping MessagingEndUserId mapping (will track all as unmapped)...")
    
    # Prepare output files
    source_basename = os.path.splitext(os.path.basename(SOURCE_FILE))[0]
    main_output_file = os.path.join(OUTPUT_DIR, f"{source_basename}_mapped.csv")
    
    # Remove existing output files
    if os.path.exists(main_output_file):
        os.remove(main_output_file)
    
    # Track unmapped MessagingEndUserId records
    unmapped_messaging_enduser = []
    
    reader = pd.read_csv(SOURCE_FILE, dtype=str, chunksize=CHUNK_SIZE)
    header_written = False
    total_rows = 0
    
    print("🔄 Processing source file in chunks...")
    
    for chunk_idx, chunk in enumerate(reader, start=1):
        chunk = chunk.fillna("")
        
        # === USER LOOKUP FOR 3 FIELDS ===
        user_fields = ["OwnerId", "CreatedById", "LastModifiedById"]
        
        for col in user_fields:
            if col in chunk.columns:
                # Apply simple lookup
                chunk[col] = chunk[col].apply(map_user_id)
                
                # Set defaults for blank/unmapped values
                if col == "OwnerId":
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_OWNER_ID
                else:  # CreatedById, LastModifiedById
                    mask = chunk[col].astype(str).str.strip() == ""
                    chunk.loc[mask, col] = DEFAULT_CREATEDBY_LASTMODIFIED_ID
        
        # === MESSAGINGCHANNEL ID - SET CONSTANT VALUE ===
        chunk["MessagingChannelId"] = MESSAGING_CHANNEL_ID
        
        # === MESSAGINGENDUSER LOOKUP ===
        if "MessagingEndUserId" in chunk.columns and dict_messaging_enduser:
            # Store original values for unmapped tracking
            original_enduser = chunk["MessagingEndUserId"].astype(str).str.strip()
            
            # Apply lookup
            chunk["MessagingEndUserId"] = chunk["MessagingEndUserId"].apply(
                lambda val: dict_messaging_enduser.get(str(val).strip().lower(), "") if str(val).strip() else ""
            )
            
            mapped_enduser = chunk["MessagingEndUserId"].astype(str).str.strip()
            
            # Track unmapped: original had value but mapping returned blank
            unmapped_mask = (original_enduser != "") & (mapped_enduser == "")
            if unmapped_mask.any():
                unmapped_rows = chunk[unmapped_mask].copy()
                # Store Id and original MessagingEndUserId value
                unmapped_rows["MessagingEndUserId"] = original_enduser[unmapped_mask].values
                unmapped_messaging_enduser.append(unmapped_rows[["Id", "MessagingEndUserId"]])
        
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
    if unmapped_messaging_enduser:
        print("\n📝 Writing unmapped MessagingEndUserId report...")
        unmapped_df = pd.concat(unmapped_messaging_enduser, ignore_index=True)
        unmapped_file = os.path.join(OUTPUT_DIR, "MessagingEndUserId_unmapped.csv")
        unmapped_df.to_csv(unmapped_file, index=False, encoding="utf-8-sig")
        print(f"   ⚠️ MessagingEndUserId: {len(unmapped_df)} unmapped → {unmapped_file}")
    
    # === SUMMARY ===
    print("\n" + "="*60)
    print("✅ MESSAGING SESSION MAPPING COMPLETED!")
    print("="*60)
    print(f"📊 Total rows processed: {total_rows}")
    print(f"📝 Main output: {main_output_file}")
    
    main_size_mb = os.path.getsize(main_output_file) / (1024 * 1024)
    print(f"📏 Output file size: {main_size_mb:.2f} MB")
    
    if unmapped_messaging_enduser:
        total_unmapped = len(pd.concat(unmapped_messaging_enduser, ignore_index=True))
        print(f"\n⚠️ Total unmapped MessagingEndUserId records: {total_unmapped}")
    else:
        print("\n✅ All MessagingEndUserId values mapped successfully!")


if __name__ == "__main__":
    main()

