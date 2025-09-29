#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd

# File paths
incidents_path = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_INCIDENTS.csv"
perps_path     = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_PERPS.csv"
source_path    = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_SOURCE.csv"

# Load CSV files
df_incidents = pd.read_csv(incidents_path, low_memory=False)
df_perps     = pd.read_csv(perps_path, low_memory=False)
df_source    = pd.read_csv(source_path, low_memory=False)

# Confirm shapes
print("Incidents:", df_incidents.shape)
print("Perpetrators:", df_perps.shape)
print("Source:", df_source.shape)


# In[7]:


import pandas as pd

cats_path = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\Perpetrators with Categories.csv"

# Try with latin1 encoding
df_cats = pd.read_csv(cats_path, encoding="latin1", low_memory=False)

print("Perpetrators with Categories:", df_cats.shape)
print(df_cats.head())


# In[9]:


import pandas as pd

cats_path = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\Perpetrators with Categories.csv"

# Load again, forcing first row as header
df_cats = pd.read_csv(cats_path, encoding="latin1", header=1)

# Remove rows where Category is cartals or gangs
df_cats_cleaned = df_cats[~df_cats['Category'].str.lower().isin(['cartel', 'gang'])]

print("Before:", df_cats.shape)
print("After:", df_cats_cleaned.shape)


# In[12]:


import pandas as pd
import re

# File paths
perps_path = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_PERPS.csv"
cats_path  = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\Perpetrators with Categories.csv"

# --- 1) Load data ---
df_perps = pd.read_csv(perps_path, low_memory=False)                 # GRID_PERPS
df_cats  = pd.read_csv(cats_path, encoding="latin1", header=1)       # Perpetrators with Categories

# --- 2) Remove Cartals and Gangs ---
df_cats = df_cats[~df_cats["Category"].str.lower().isin(["cartals", "gangs"])]

# --- 3) Normalize names for matching ---
def normalize_name(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = s.replace("’", "'").replace("‘", "'").replace("`", "'")      # unify apostrophes
    s = re.sub(r"[^A-Za-z0-9\s\-\(\)'&]", " ", s)                   # remove special chars
    s = re.sub(r"\s+", " ", s).strip().lower()                      # clean spaces + lowercase
    return s

# Create FTO list
fto_names = set(df_cats["Perpetrator"].map(normalize_name).tolist())

# --- 4) Flag FTOs in GRID_PERPS ---
perp_norm = df_perps["perp_name"].map(normalize_name)
df_perps["is_fto"] = perp_norm.isin(fto_names)
df_perps["fto_status"] = df_perps["is_fto"].map({True: "FTO", False: "Non-FTO"})

# --- 5) Save output ---
out_path = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_PERPS_FTO_flag.csv"
df_perps.to_csv(out_path, index=False)

print("✅ Processing complete!")
print("Output saved to:", out_path)
print(df_perps["fto_status"].value_counts())


# In[15]:


import pandas as pd

# -----------------------
# File paths (edit if needed)
# -----------------------
incidents_path   = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_INCIDENTS.csv"
perps_flag_path  = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_PERPS_FTO_flag.csv"
out_path         = r"C:\Users\veges\OneDrive\Desktop\DAEN 690\GRID_as_csv_tables\20250825-GRID_INCIDENTS_with_FTO_flags.csv"

# -----------------------
# Load data
# -----------------------
df_inc = pd.read_csv(incidents_path, low_memory=False)
df_pf  = pd.read_csv(perps_flag_path, low_memory=False)  # must contain fto_status + incident-id column

# -----------------------
# Identify the incident-id columns in each file (robust to naming)
# -----------------------
def find_col(df, candidates):
    cand_lower = [c.lower() for c in candidates]
    # exact lower-name match
    for col in df.columns:
        if col.lower() in cand_lower:
            return col
    # fallback: contains pattern
    for col in df.columns:
        if any(tok in col.lower() for tok in ["incident_id", "perp_incident_id", "unique_incident"]):
            return col
    raise KeyError(f"Could not find any of these columns in the DataFrame: {candidates}")

# likely names in each file
inc_id_col_candidates = ["incident_id", "unique_incident_id", "perp_incident_id"]
perp_id_col_candidates = ["perp_incident_id", "incident_id", "unique_incident_id"]

inc_id_col  = find_col(df_inc, inc_id_col_candidates)
perp_id_col = find_col(df_pf, perp_id_col_candidates)

# sanity check for fto_status
if "fto_status" not in df_pf.columns:
    raise KeyError("Column 'fto_status' not found in the perps flag file. Re-generate the flagged file first.")

# -----------------------
# Aggregate perp-level flags to the incident level
# -----------------------
# Normalize fto_status values just in case
df_pf["fto_status"] = df_pf["fto_status"].astype(str).str.strip()

grp = df_pf.groupby(perp_id_col).agg(
    incident_has_fto     = ("fto_status", lambda s: (s == "FTO").any()),
    incident_has_non_fto = ("fto_status", lambda s: (s == "Non-FTO").any())
).reset_index()

def label_row(row):
    fto = bool(row["incident_has_fto"])
    non = bool(row["incident_has_non_fto"])
    if fto and not non:
        return "FTO only"
    if non and not fto:
        return "Non-FTO only"
    if fto and non:
        return "Mixed (FTO & Non-FTO)"
    return "Unknown"

grp["incident_fto_label"] = grp.apply(label_row, axis=1)

# -----------------------
# Merge incident-level flags into the incidents table
# -----------------------
df_inc_merged = df_inc.merge(
    grp.rename(columns={perp_id_col: "_incident_id_key"}),
    left_on=inc_id_col, right_on="_incident_id_key", how="left"
).drop(columns=["_incident_id_key"])

# Fill missing (no perps found for that incident) as Unknown/False
df_inc_merged["incident_has_fto"]      = df_inc_merged["incident_has_fto"].fillna(False)
df_inc_merged["incident_has_non_fto"]  = df_inc_merged["incident_has_non_fto"].fillna(False)
df_inc_merged["incident_fto_label"]    = df_inc_merged["incident_fto_label"].fillna("Unknown")

# -----------------------
# Save
# -----------------------
df_inc_merged.to_csv(out_path, index=False)

# -----------------------
# Quick summary
# -----------------------
print("✅ Done. Saved to:", out_path)
print("Counts by incident_fto_label:")
print(df_inc_merged["incident_fto_label"].value_counts(dropna=False))

