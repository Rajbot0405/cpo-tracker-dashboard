import pandas as pd
import numpy as np

def clean(df, columns):
    for col in columns:
        df[col] = df[col].astype(str).str.replace(u'\xa0', '', regex=False)
        df[col] = df[col].str.strip().str.replace(r'\s+', '', regex=True).str.upper()
    return df

def process_tracker_data(s2q_df, contract_df, quotes_df):
    # --- Rename and clean S2Q ---
    s2q_df.rename(columns={
        'Site ID*\n(candidate specific)': 'Site ID',
        'Root Cause ': 'Root Cause'
    }, inplace=True)
    s2q_df = clean(s2q_df, ['Site ID', 'Quote Id'])

    # --- Rename and clean Contract Dump ---
    contract_df.rename(columns={'Quote ID': 'Quote Id'}, inplace=True)
    contract_df = clean(contract_df, ['Site ID', 'Quote Id'])

    # --- Rename and clean Quotes ---
    quotes_df.rename(columns={
        'ID': 'Quote Id',
        'Status': 'Quote Status',
        'Recent comment': 'Quote Recent Comment',
        'Assignee': 'Quote Assignee',
        'Assignee Organisation': 'Quote Assignee Organisation'
    }, inplace=True)
    quotes_df = clean(quotes_df, ['Quote Id'])

    # --- Merge contracts with S2Q one-to-one by Site ID + Quote Id ---
    merged_chunks = []
    contract_groups = contract_df.groupby(['Site ID', 'Quote Id'])
    s2q_groups = s2q_df.groupby(['Site ID', 'Quote Id'])

    for key, s2q_rows in s2q_groups:
        s2q_rows = s2q_rows.reset_index(drop=True).copy()
        contract_rows = contract_groups.get_group(key).reset_index(drop=True).copy() if key in contract_groups.groups else pd.DataFrame()

        row_count = min(len(s2q_rows), len(contract_rows)) if not contract_rows.empty else 0

        if row_count > 0:
            combined = pd.concat([
                s2q_rows.iloc[:row_count].reset_index(drop=True),
                contract_rows.iloc[:row_count].drop(columns=['Site ID', 'Quote Id'], errors='ignore').reset_index(drop=True)
            ], axis=1)
            merged_chunks.append(combined)

        if len(s2q_rows) > row_count:
            unmatched = s2q_rows.iloc[row_count:].copy()
            unmatched.loc[:, ['Contract ID', 'PO #', 'PR Total($)', 'PO Status', 'Contract Status']] = np.nan
            merged_chunks.append(unmatched)

    # --- Ensure unique column names before concat ---
    for i in range(len(merged_chunks)):
        cols = pd.Series(merged_chunks[i].columns)
        deduped_cols = cols.where(~cols.duplicated(), cols + '.' + cols.duplicated(keep='first').astype(str))
        merged_chunks[i].columns = deduped_cols

    final_df = pd.concat(merged_chunks, ignore_index=True)

    # --- Merge Quotes metadata ---
    final_df = final_df.merge(quotes_df, on="Quote Id", how="left")

    # --- Apply Comments logic ---
    conditions = [
        (pd.notna(final_df['PO #']) & (final_df['PO #'] != "N.A.")),
        (final_df['Contract Status'].isin(["Assigned", "Accepted (by Supplier)", "Unassigned (by Supplier)"]) &
         (final_df['PO #'].isna() | (final_df['PO #'] == "N.A."))),
        (final_df['Root Cause'].isin(["Submitted", "Resubmitted", "Submitted with WBS Error"]) &
         (final_df['Final Status'] == "In Progress")),
        (final_df['Final Status'] == "Terminated"),
        ((final_df['Final Status'] == "Completed") & (final_df['Contract Status'] == "Acknowledgement Pending")),
        (~final_df['Root Cause'].isin(["Submitted", "Resubmitted", "Submitted with WBS Error"])),
        (final_df['Contract Status'] == "Supplier Rejected"),
        ((final_df['Final Status'] == "Completed") & (final_df['Contract ID'].isna()))
    ]

    choices = [
        "PO Generated",
        "Pending with TMO - SW contract accepted PO creation pending",
        "Pending with TMO - Need to Process submitted Quote",
        "TMO Rejected the Quote in S2Q level",
        "Pending with SW team - Need to Process Scopeworker contract",
        "S2Q Not processed due to issue (refer Remarks and Root Cause column)",
        "SW contract Rejected",
        "TMO Rejected the Quote in S2Q level"
    ]

    final_df['Comments'] = np.select(conditions, choices, default="Check tool live status")
    final_df.loc[final_df['Comments'] == "TMO Rejected the Quote in S2Q level", 'Final Status'] = "Terminated"

    # --- Move quote metadata to end ---
    quote_columns = ["Quote Status", "Quote Recent Comment", "Quote Assignee", "Quote Assignee Organisation"]
    for col in quote_columns:
        if col in final_df.columns:
            final_df = final_df[[c for c in final_df.columns if c != col] + [col]]

    return final_df
