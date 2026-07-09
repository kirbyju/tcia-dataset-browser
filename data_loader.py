# data_loader.py (Updated)
import pandas as pd
import streamlit as st

# The filename where the synced data is stored
DATA_FILE = "tcia_master_data.parquet"
DOWNLOADS_FILE = "downloads_cache.parquet"

import os

def get_mtime(filepath):
    if os.path.exists(filepath):
        return os.path.getmtime(filepath)
    return 0

@st.cache_data(show_spinner="Loading TCIA data...")
def get_master_dataframe(mtime):
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
def get_downloads_dataframe(mtime):
    """
    Loads the pre-processed downloads DataFrame from a local Parquet file.
    """
    try:
        df = pd.read_parquet(DOWNLOADS_FILE)
        return df
    except FileNotFoundError:
        st.error(f"FATAL ERROR: The data file '{DOWNLOADS_FILE}' was not found.")
        st.info("Please run the `sync_data.py` script first to download and process the data from the TCIA API.")
        # Stop the app from running further if the data file is missing
        st.stop()
