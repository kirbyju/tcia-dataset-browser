# app.py (Corrected with robust single-mask filtering)
import streamlit as st
import pandas as pd
from data_loader import get_master_dataframe
import datetime
import numpy as np

# --- CONFIGURATION ---
BASE_URL = "https://tcia-streamlit.duckdns.org/dataset-browser/"

# --- Helper Functions ---
def get_unique_values_from_column(df, column_name):
    if column_name not in df.columns: return []
    if df[column_name].apply(lambda x: isinstance(x, list)).any():
        valid_lists = df[column_name][df[column_name].apply(lambda x: isinstance(x, list))]
        unique_values = set(v for v_list in valid_lists for v in v_list if v)
    else:
        unique_values = set(df[column_name].unique())
    return sorted(list(unique_values - {''}))

def format_tags(tags_list):
    if not isinstance(tags_list, list): return "N/A"
    if not tags_list: return "N/A"
    return " | ".join(f"`{tag}`" for tag in tags_list)

def get_unique_download_values(downloads_info, key):
    if not isinstance(downloads_info, list) or not downloads_info:
        return []
    values = set()
    for download in downloads_info:
        if isinstance(download, dict) and download.get(key):
            # Value could be a list or a single item
            item = download[key]
            if isinstance(item, list):
                values.update(val for val in item if val)
            else:
                values.add(item)
    return sorted(list(values))

# --- Page Configuration ---
st.set_page_config(page_title="TCIA Dataset Explorer", page_icon="🔬", layout="wide")

# --- Load and Prepare Data ---
df = get_master_dataframe()
list_cols = ['cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program', 'related_datasets', 'downloads_info']
for col in list_cols:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: list(x) if isinstance(x, np.ndarray) else x if isinstance(x, list) else [])
df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')

# --- Create a lookup dictionary for related datasets ---
id_to_title = pd.Series(df.short_title.values, index=df.id).to_dict()

# --- Pre-calculate filter options ---
# Special handling for Data Type filter, which comes from downloads
all_download_data_types = sorted(list(set(
    dtype
    for download_list in df['downloads_info'] if isinstance(download_list, list)
    for download in download_list if isinstance(download, dict) and 'data_type' in download and download['data_type']
    for dtype in (download['data_type'] if isinstance(download['data_type'], list) else [download['data_type']])
)))


# --- Process URL Query Parameters to set defaults ---
query_params = st.query_params.to_dict()
defaults = {key: value[0].split(',') if isinstance(value, list) else value.split(',') for key, value in query_params.items()}

# --- Sidebar ---
st.sidebar.image("https://www.cancerimagingarchive.net/wp-content/uploads/2021/06/TCIA-Logo-01.png")
st.sidebar.header("Filter Datasets")

search_query = st.sidebar.text_input("Search all fields...", help="Performs a case-insensitive search.", value=','.join(defaults.get('q', [''])))
FILTERS = [
    ("Data Type", "data_types"), ("Cancer Type", "cancer_types"),
    ("Cancer Location", "cancer_locations"), ("Supporting Data", "supporting_data"),
    ("Program", "program"), ("Access", "access_type")
]
selected_filters = {}
for label, column in FILTERS:
    if column == "data_types":
        options = all_download_data_types
    else:
        options = get_unique_values_from_column(df, column)

    if options:
        valid_defaults = [v for v in defaults.get(column, []) if v in options]
        selected_filters[column] = st.sidebar.multiselect(label, options, default=valid_defaults)

df['number_of_subjects'] = pd.to_numeric(df['number_of_subjects'], errors='coerce').fillna(0).astype(int)
min_subjects, max_subjects = int(df['number_of_subjects'].min()), int(df['number_of_subjects'].max())
default_subjects = defaults.get('subject_range', [f"{min_subjects},{max_subjects}"])[0].split(',')
default_subject_range = (int(default_subjects[0]), int(default_subjects[1])) if len(default_subjects) == 2 else (min_subjects, max_subjects)
subject_range = st.sidebar.slider("Number of Subjects", min_subjects, max_subjects, default_subject_range)

valid_dates = df['date_updated'].dropna()
min_date, max_date = valid_dates.min().date(), valid_dates.max().date()
default_dates = defaults.get('date_range', [f"{min_date},{max_date}"])[0].split(',')
default_date_range = (datetime.datetime.strptime(default_dates[0], '%Y-%m-%d').date(), datetime.datetime.strptime(default_dates[1], '%Y-%m-%d').date()) if len(default_dates) == 2 else (min_date, max_date)
date_range = st.sidebar.date_input("Date Updated", default_date_range, min_date, max_date)

# --- ROBUST FILTERING LOGIC ---
# Start with a mask that includes all rows
final_mask = pd.Series(True, index=df.index)

# Apply free-text search
if search_query:
    search_cols = ['title', 'short_title', 'summary', 'citation', 'program', 'data_types', 'cancer_types', 'cancer_locations']
    existing_search_cols = [col for col in search_cols if col in df.columns]
    search_text = df[existing_search_cols].astype(str).agg(' '.join, axis=1)
    final_mask &= search_text.str.contains(search_query, case=False, na=False)

# Apply categorical filters
for column, selected_values in selected_filters.items():
    if selected_values:
        if column == 'data_types':
            def has_selected_data_type(downloads_list):
                if not isinstance(downloads_list, list):
                    return False
                for download in downloads_list:
                    if isinstance(download, dict):
                        dtypes = download.get('data_type', [])
                        if not isinstance(dtypes, list):
                            dtypes = [dtypes]
                        if any(dtype in selected_values for dtype in dtypes if dtype):
                            return True
                return False
            mask = df['downloads_info'].apply(has_selected_data_type)
        elif df[column].apply(lambda x: isinstance(x, list)).any():
            mask = df[column].apply(lambda lst: any(v in selected_values for v in lst))
        else:
            mask = df[column].isin(selected_values)
        final_mask &= mask

# Apply range filters
final_mask &= df['number_of_subjects'].between(subject_range[0], subject_range[1])
if len(date_range) == 2:
    # Ensure we handle NaT dates gracefully
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1])
    final_mask &= df['date_updated'].between(start_date, end_date, inclusive='both')

# Apply the final combined mask to the original DataFrame
filtered_df = df[final_mask]

# --- Main Panel ---
st.title("🔬 TCIA Dataset Explorer")
st.markdown("An interactive tool to filter and find datasets from The Cancer Imaging Archive.")
st.write(f"**Found {len(filtered_df)} matching datasets.**")

share_params = {}
if search_query: share_params['q'] = search_query
for column, selected_values in selected_filters.items():
    if selected_values: share_params[column] = ",".join(selected_values)
if subject_range != (min_subjects, max_subjects):
    share_params['subject_range'] = f"{subject_range[0]},{subject_range[1]}"
if date_range != (min_date, max_date):
    share_params['date_range'] = f"{date_range[0].strftime('%Y-%m-%d')},{date_range[1].strftime('%Y-%m-%d')}"

if share_params:
    query_string = "&".join([f"{key}={','.join(value) if isinstance(value, list) else value}" for key, value in share_params.items()])
    share_url = f"{BASE_URL}?{query_string}"
    st.markdown("**Share this query:**")
    st.code(share_url)

st.markdown("---")

PAGE_SIZE = 25
if 'current_page' not in st.session_state: st.session_state.current_page = 1
total_results = len(filtered_df)
total_pages = max(1, (total_results // PAGE_SIZE) + (1 if total_results % PAGE_SIZE > 0 else 0))
st.session_state.current_page = min(st.session_state.current_page, total_pages)
start_index = (st.session_state.current_page - 1) * PAGE_SIZE
end_index = start_index + PAGE_SIZE
paginated_df = filtered_df.sort_values(by="date_updated", ascending=False).iloc[start_index:end_index]

if paginated_df.empty:
    st.warning("No datasets match the current filter criteria. Please broaden your search.")
else:
    for _, row in paginated_df.iterrows():
        display_title = f"{row.get('short_title', '')} | {row['title']}" if row.get('short_title') else row['title']
        st.markdown(f"### [{display_title}]({row['link']})")

        col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1.5])
        col1.markdown(f"**DOI:** `{row.get('doi', 'N/A')}`")
        col2.markdown(f"**Program:** {format_tags(row.get('program'))}")
        col3.markdown(f"**Type:** `{row['dataset_type']}`")
        col4.markdown(f"**Subjects:** `{row['number_of_subjects']}`")
        col5.markdown(f"**Updated:** `{row['date_updated'].strftime('%Y-%m-%d')}`")

        # --- Download-derived info ---
        downloads_info = row.get('downloads_info', [])
        data_types = get_unique_download_values(downloads_info, 'data_type')
        data_categories = get_unique_download_values(downloads_info, 'download_type')
        licenses = get_unique_download_values(downloads_info, 'data_license')

        colA, colB = st.columns(2)
        colA.markdown(f"**Data Type(s):** {format_tags(data_types)}")
        colB.markdown(f"**Supporting Data:** {format_tags(row.get('supporting_data'))}")

        colC, colD = st.columns(2)
        colC.markdown(f"**Data Categories:** {format_tags(data_categories)}")
        colD.markdown(f"**Licenses:** {format_tags(licenses)}")


        st.markdown(f"**Cancer Type(s):** {format_tags(row.get('cancer_types'))}")
        st.markdown(f"**Cancer Location(s):** {format_tags(row.get('cancer_locations'))}")

        related_ids = row.get('related_datasets', [])
        related_titles = [id_to_title.get(rid, f"ID: {rid}") for rid in related_ids]
        st.markdown(f"**Related Datasets:** {format_tags(related_titles)}")

        if row.get('citation') or row.get('summary') or downloads_info:
            with st.expander("View Citation, Abstract, and Downloads"):
                if row.get('citation'):
                    st.markdown(f"**Citation:** {row['citation']}")
                if row.get('summary'):
                    if row.get('citation'): st.markdown("---")
                    st.markdown(f"**Abstract:** {row['summary']}")

                if downloads_info:
                    if row.get('citation') or row.get('summary'): st.markdown("---")
                    st.markdown("**Available Downloads:**")
                    for d in downloads_info:
                        title = d.get('download_title', 'N/A')
                        url = d.get('download_url', '')
                        st.markdown(f"- **[{title}]({url})**")

                        details_cols = st.columns([2,1,1,1])
                        details_cols[0].markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;*License:* `{d.get('data_license', 'N/A')}`")
                        details_cols[1].markdown(f"*Type:* `{d.get('download_type', 'N/A')}`")
                        details_cols[2].markdown(f"*Size:* `{d.get('download_size', 'N/A')} {d.get('download_size_unit', '')}`")
                        details_cols[3].markdown(f"*Updated:* `{d.get('date_updated', 'N/A')}`")

                        if d.get('subjects'):
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;*Subjects:* `{d.get('subjects')}`")
                        if d.get('download_requirements'):
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;*Requirements:* `{d.get('download_requirements')}`")
        st.markdown("---")

    st.write(f"Page {st.session_state.current_page} of {total_pages}")
    prev_col, next_col = st.columns(2)
    if prev_col.button("⬅️ Previous Page", use_container_width=True, disabled=(st.session_state.current_page <= 1)):
        st.session_state.current_page -= 1
        st.rerun()
    if next_col.button("Next Page ➡️", use_container_width=True, disabled=(st.session_state.current_page >= total_pages)):
        st.session_state.current_page += 1
        st.rerun()
