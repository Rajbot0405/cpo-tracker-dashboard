import streamlit as st
import pandas as pd
import io
from logic import process_tracker_data

st.set_page_config(page_title="E2E CPO Tracker", layout="wide")
st.title("📊 E2E CPO Master Tracker Dashboard")

# --- File Upload Section ---
st.sidebar.header("📁 Upload Excel Files")
s2q_file = st.sidebar.file_uploader("S2Q Master Tracker", type="xlsx")
contract_file = st.sidebar.file_uploader("Contract Dump", type="xlsx")
quotes_file = st.sidebar.file_uploader("Quotes File", type="xlsx")

if s2q_file and contract_file and quotes_file:
    with st.spinner("🔄 Processing data..."):
        s2q_df = pd.read_excel(s2q_file, header=1)
        contract_df = pd.read_excel(contract_file)
        quotes_df = pd.read_excel(quotes_file)
        final_df = process_tracker_data(s2q_df, contract_df, quotes_df)

    st.success("✅ Tracker built successfully")

    # --- Slicer Section ---
    st.sidebar.header("🔎 Filter Data")
    site_values = sorted(final_df['Site ID'].dropna().unique())
    quote_values = sorted(final_df['Quote Id'].dropna().unique())
    region_values = sorted(final_df['Region'].dropna().unique())
    market_values = sorted(final_df['Market'].dropna().unique())
    contract_values = sorted(final_df['Contract ID'].dropna().unique())
    category_values = sorted(final_df['Category'].dropna().unique())

    selected_sites = st.sidebar.multiselect("Site ID", site_values)
    selected_quotes = st.sidebar.multiselect("Quote ID", quote_values)
    selected_regions = st.sidebar.multiselect("Region", region_values)
    selected_markets = st.sidebar.multiselect("Market", market_values)
    selected_contracts = st.sidebar.multiselect("Contract ID", contract_values)
    selected_categories = st.sidebar.multiselect("Category", category_values)

    # --- Apply Filters ---
    filtered_df = final_df.copy()
    if selected_sites:
        filtered_df = filtered_df[filtered_df['Site ID'].isin(selected_sites)]
    if selected_quotes:
        filtered_df = filtered_df[filtered_df['Quote Id'].isin(selected_quotes)]
    if selected_regions:
        filtered_df = filtered_df[filtered_df['Region'].isin(selected_regions)]
    if selected_markets:
        filtered_df = filtered_df[filtered_df['Market'].isin(selected_markets)]
    if selected_contracts:
        filtered_df = filtered_df[filtered_df['Contract ID'].isin(selected_contracts)]
    if selected_categories:
        filtered_df = filtered_df[filtered_df['Category'].isin(selected_categories)]

    # --- Preview Filtered Tracker ---
    st.subheader("🔍 Preview Enriched Tracker")
    st.dataframe(filtered_df, use_container_width=True)

    # --- Download Filtered Output ---
    output = io.BytesIO()
    filtered_df.to_excel(output, index=False)
    st.download_button(
        label="📥 Download Filtered Tracker",
        data=output.getvalue(),
        file_name="Filtered_E2E_CPO_Tracker.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("⬅️ Upload all three Excel files in the sidebar to begin.")
