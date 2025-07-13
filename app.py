# app.py (Updated with new UI layout)
import streamlit as st
import pandas as pd
from data_loader import get_master_dataframe
import datetime
import numpy as np

# --- Helper Functions ---
def get_unique_values_from_column(df, column_name):
    if column_name not in df.columns: return []
    valid_lists = df[column_name][df[column_name].apply(lambda x: isinstance(x, list))]
    unique_values = set(v for v_list in valid_lists for v in v_list if v)
    return sorted(list(unique_values))

def format_tags(tags_list):
    if not isinstance(tags_list, list): return "N/A"
    if not tags_list: return "N/A"
    return " | ".join(f"`{tag}`" for tag in tags_list)

# --- Page Configuration ---
st.set_page_config(page_title="TCIA Dataset Explorer", page_icon="🔬", layout="wide")

# --- Load and Prepare Data ---
df = get_master_dataframe()
list_cols = ['cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program', 'related_datasets']
for col in list_cols:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: list(x) if isinstance(x, np.ndarray) else x if isinstance(x, list) else [])
df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')

# --- Sidebar ---
st.sidebar.image("https://www.cancerimagingarchive.net/wp-content/uploads/2021/06/TCIA-Logo-01.png")
st.sidebar.header("Filter Datasets")

search_query = st.sidebar.text_input("Search all fields...", help="Performs a case-insensitive search.")
FILTERS = [
    ("Data Type", "data_types"), ("Cancer Type", "cancer_types"),
    ("Cancer Location", "cancer_locations"), ("Supporting Data", "supporting_data"),
    ("Program", "program")
]
selected_filters = {}
for label, column in FILTERS:
    options = get_unique_values_from_column(df, column)
    if options:
        selected_filters[column] = st.sidebar.multiselect(label, options)

df['number_of_subjects'] = pd.to_numeric(df['number_of_subjects'], errors='coerce').fillna(0).astype(int)
min_subjects, max_subjects = int(df['number_of_subjects'].min()), int(df['number_of_subjects'].max())
subject_range = st.sidebar.slider("Number of Subjects", min_subjects, max_subjects, (min_subjects, max_subjects))

valid_dates = df['date_updated'].dropna()
min_date, max_date = valid_dates.min().date(), valid_dates.max().date()
date_range = st.sidebar.date_input("Date Updated", (min_date, max_date), min_date, max_date)

# --- Filtering Logic ---
filtered_df = df.dropna(subset=['date_updated']).copy()
if search_query:
    search_cols = ['title', 'short_title', 'summary', 'citation', 'program', 'data_types', 'cancer_types', 'cancer_locations']
    filtered_df['search_text'] = filtered_df[search_cols].astype(str).agg(' '.join, axis=1)
    filtered_df = filtered_df[filtered_df['search_text'].str.contains(search_query, case=False, na=False)]
for column, selected_values in selected_filters.items():
    if selected_values:
        filtered_df = filtered_df[filtered_df[column].apply(lambda lst: any(v in selected_values for v in lst))]
filtered_df = filtered_df[filtered_df['number_of_subjects'].between(subject_range[0], subject_range[1])]
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = filtered_df[filtered_df['date_updated'].between(start_date, end_date)]

# --- Main Panel ---
st.title("🔬 TCIA Dataset Explorer")
st.markdown("An interactive tool to filter and find datasets from The Cancer Imaging Archive.")
st.write(f"**Found {len(filtered_df)} matching datasets.**")
st.markdown("---")

if filtered_df.empty:
    st.warning("No datasets match the current filter criteria. Please broaden your search.")
else:
    for _, row in filtered_df.sort_values(by="date_updated", ascending=False).iterrows():
        # 2. New title format
        display_title = f"{row.get('short_title', '')} | {row['title']}" if row.get('short_title') else row['title']
        st.markdown(f"### [{display_title}]({row['link']})")

        # 3 & 4. New 5-column metadata row
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.markdown(f"**DOI:** `{row.get('doi', 'N/A')}`")
        col2.markdown(f"**Program:** {format_tags(row.get('program'))}")
        col3.markdown(f"**Type:** `{row['dataset_type']}`")
        col4.markdown(f"**Subjects:** `{row['number_of_subjects']}`")
        col5.markdown(f"**Updated:** `{row['date_updated'].strftime('%Y-%m-%d')}`")

        # 3. New Data Type / Supporting Data row
        colA, colB = st.columns(2)
        colA.markdown(f"**Data Type(s):** {format_tags(row.get('data_types'))}")
        colB.markdown(f"**Supporting Data:** {format_tags(row.get('supporting_data'))}")

        st.markdown(f"**Cancer Type(s):** {format_tags(row.get('cancer_types'))}")
        st.markdown(f"**Cancer Location(s):** {format_tags(row.get('cancer_locations'))}")
        # 5. New Related Datasets field
        st.markdown(f"**Related Datasets:** {format_tags(row.get('related_datasets'))}")

        # 1. Citation moved into expander
        if row.get('citation') or row.get('summary'):
            with st.expander("View Citation and Abstract"):
                if row.get('citation'):
                    st.markdown(f"**Citation:** {row['citation']}")
                    if row.get('summary'): st.markdown("---")
                if row.get('summary'):
                    st.markdown(f"**Abstract:** {row['summary']}")

        st.markdown("---")
