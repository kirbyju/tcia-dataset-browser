# data_loader.py (Updated)
import pandas as pd
import streamlit as st

# The filename where the synced data is stored
DATA_FILE = "tcia_master_data.parquet"
DOWNLOADS_FILE = "downloads_cache.parquet"

@st.cache_data(show_spinner="Loading TCIA data...")
def get_master_dataframe():
    """
    Loads the pre-processed master DataFrame from a local Parquet file.
    """
    try:
        df = pd.read_parquet(DATA_FILE)
        return df
    except FileNotFoundError:
        st.error(f"FATAL ERROR: The data file '{DATA_FILE}' was not found.")
        st.info("Please run the `sync_data.py` script first to download and process the data from the TCIA API.")
        # Stop the app from running further if the data file is missing
        st.stop()

@st.cache_data(show_spinner="Loading TCIA downloads data...")
def get_downloads_dataframe():
    """
    Loads the pre-processed downloads DataFrame from a local Parquet file.
    """
    try:
        df = pd.read_parquet(DOWNLOADS_FILE)
        # Ensure the 'id' column is of a numeric type for merging
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        return df
    except FileNotFoundError:
        st.error(f"FATAL ERROR: The downloads data file '{DOWNLOADS_FILE}' was not found.")
        st.info("Please run the `sync_data.py` script first to download and process the data.")
        st.stop()
