import pandas as pd
import os

# === CONFIG ===
folder = r"C:\Users\ivc21324adm\OneDrive - InfoVision, Inc\GC-DM-Production Data\ECommerce\Contact\DigitalProd-SourceFiles"
input_file = os.path.join(folder, "Deduplication_FinalMergedReport.csv")

# Lookup files
user_lkp_path = r"C:\Users\ivc21324adm\OneDrive - InfoVision, Inc\GC-DM-Production Data\ECommerce\Users\DigitalVsComponent_User_Lkp_File.csv"
country_lkp_path = os.path.join(folder, "MailingBillingCountry_RecordTypeId.csv")
account_lkp_path = os.path.join(folder, "Contact_MyArrow_V1.csv")

# Output paths
detail_csv = input_file.replace(".csv", "_DetailReport.csv")
summary_csv = input_file.replace(".csv", "_SummaryReport.csv")

default_record_type_id = '012G0000000wlBvIAI'

# === STEP 1: Load lookup tables ===
user_df = pd.read_csv(user_lkp_path, dtype=str)
country_df = pd.read_csv(country_lkp_path, dtype=str)
account_df = pd.read_csv(account_lkp_path, dtype=str)

legacy_map = dict(zip(user_df['Legacy_SF_Record_ID__c'], user_df['Id']))
country_map = dict(zip(country_df['MailingCountryCode'], country_df['RecordTypeId']))
account_ids = set(account_df['Id'].dropna())

# === STEP 2: Load main file ===
df = pd.read_csv(input_file, dtype=str, encoding='utf-8-sig')
df['MailingCountry'] = df['MailingCountry'].fillna('').str.strip()

# --- Create Lkp + Flag fields (without overwriting source) ---
for col in ['OwnerId', 'CreatedById', 'LastModifiedById']:
    df[f"{col}_Lkp"] = df[col].map(legacy_map).fillna('')
    df[f"{col}_Flag"] = df[col].apply(lambda x: "Y" if x in legacy_map else "N")

# RecordTypeId is derived from MailingCountry
df['RecordTypeId_Lkp'] = df['MailingCountry'].map(country_map).fillna(default_record_type_id)
df['RecordTypeId_Flag'] = df['MailingCountry'].apply(lambda x: "Y" if x in country_map else "N")

# AccountId lookup
df['AccountId_Lkp'] = df['Id'].apply(lambda x: "001Vq00000bXYaIIAW" if x in account_ids else "001Vq00000bXUGZIA4")
df['AccountId_Flag'] = df['Id'].apply(lambda x: "Y" if x in account_ids else "N")

# Rename Id column
df.rename(columns={'Id': 'Legacy_SF_Record_ID__c'}, inplace=True)

# === STEP 3: Build Summary counts ===
summary = {
    "OwnerId": (df['OwnerId_Flag'] == "N").sum(),
    "CreatedById": (df['CreatedById_Flag'] == "N").sum(),
    "LastModifiedById": (df['LastModifiedById_Flag'] == "N").sum(),
    "RecordTypeId": (df['RecordTypeId_Flag'] == "N").sum(),
    "AccountId": (df['AccountId_Flag'] == "N").sum()
}
summary_df = pd.DataFrame(list(summary.items()), columns=["Field","UnmatchedCount"])

# === STEP 4: Select mandatory + lookup fields for DetailReport ===
detail_fields = [
    "Legacy_SF_Record_ID__c",
    "MailingCountry",  # source for RecordTypeId
    "OwnerId", "OwnerId_Lkp", "OwnerId_Flag",
    "CreatedById", "CreatedById_Lkp", "CreatedById_Flag",
    "LastModifiedById", "LastModifiedById_Lkp", "LastModifiedById_Flag",
    "RecordTypeId_Lkp", "RecordTypeId_Flag",
    "AccountId", "AccountId_Lkp", "AccountId_Flag"
]
detail_df = df[detail_fields]

# === STEP 5: Save reports as CSV ===
detail_df.to_csv(detail_csv, index=False, encoding="utf-8-sig")
summary_df.to_csv(summary_csv, index=False, encoding="utf-8-sig")

print(f"📁 Detail report saved → {detail_csv}")
print(f"📁 Summary report saved → {summary_csv}")
