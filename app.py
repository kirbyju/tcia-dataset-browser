# app.py
import streamlit as st
import pandas as pd
import numpy as np
from data_loader import get_master_dataframe, get_downloads_dataframe, get_mtime, DATA_FILE, DOWNLOADS_FILE
import datetime

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

def format_tags(tags_list, use_code_style=True):
    if isinstance(tags_list, np.ndarray):
        tags_list = tags_list.tolist()
    if not isinstance(tags_list, list): return "N/A"
    if not tags_list: return "N/A"
    if use_code_style:
        return " | ".join(f"`{tag}`" for tag in tags_list)
    return " | ".join(str(tag) for tag in tags_list)

def format_tags_html(tags_list):
    if isinstance(tags_list, np.ndarray):
        tags_list = tags_list.tolist()
    if not isinstance(tags_list, list): return "N/A"
    if not tags_list: return "N/A"
    return " | ".join(str(tag) for tag in tags_list)

# --- Page Configuration ---
st.set_page_config(page_title="TCIA Dataset Explorer", page_icon="🔬", layout="wide")

# --- Load and Prepare Data ---
df = get_master_dataframe(get_mtime(DATA_FILE))
downloads_df = get_downloads_dataframe(get_mtime(DOWNLOADS_FILE))

# --- Verify columns exist ---
required_df_cols = ['id', 'licenses', 'supporting_data', 'data_category', 'data_types', 'program', 'cancer_types', 'cancer_locations']
required_downloads_cols = ['parent_id', 'download_title', 'download_url', 'search_url', 'download_size', 'download_size_unit', 'download_types', 'license_label']

missing_df = [col for col in required_df_cols if col not in df.columns]
missing_downloads = [col for col in required_downloads_cols if col not in downloads_df.columns]

if missing_df or missing_downloads:
    st.error("Data integrity error: The parquet files are missing required columns.")
    if missing_df: st.info(f"Missing master data columns: {', '.join(missing_df)}")
    if missing_downloads: st.info(f"Missing downloads data columns: {', '.join(missing_downloads)}")
    st.warning("This may happen if the app was updated but the data was not re-synced. Please run `python sync_data.py` to refresh your data.")
    st.stop()

list_cols = ['cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program', 'related_datasets', 'licenses', 'data_category']
for col in list_cols:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: list(x) if isinstance(x, np.ndarray) else x if isinstance(x, list) else [])
df['date_updated'] = pd.to_datetime(df['date_updated'], errors='coerce')

# --- Process URL Query Parameters to set defaults ---
query_params = st.query_params.to_dict()
defaults = {key: value[0].split(',') if isinstance(value, list) else value.split(',') for key, value in query_params.items()}

# --- Sidebar ---
st.sidebar.image("https://www.cancerimagingarchive.net/wp-content/uploads/2021/06/TCIA-Logo-01.png")
st.sidebar.header("Filter Datasets")

search_query = st.sidebar.text_input("Search all fields...", help="Performs a case-insensitive search.", value=','.join(defaults.get('q', [''])))
FILTERS = [
    ("Data Type", "data_types"), ("Data Category", "data_category"),
    ("Cancer Type", "cancer_types"), ("Cancer Location", "cancer_locations"),
    ("External Resources", "supporting_data"), ("Program", "program"),
    ("License", "licenses")
]
selected_filters = {}
for label, column in FILTERS:
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
final_mask = pd.Series(True, index=df.index)
if search_query:
    search_cols = ['title', 'short_title', 'summary', 'citation', 'program', 'data_types', 'cancer_types', 'cancer_locations']
    existing_search_cols = [col for col in search_cols if col in df.columns]
    search_text = df[existing_search_cols].astype(str).agg(' '.join, axis=1)
    final_mask &= search_text.str.contains(search_query, case=False, na=False)

for column, selected_values in selected_filters.items():
    if selected_values:
        if df[column].apply(lambda x: isinstance(x, list)).any():
            mask = df[column].apply(lambda lst: any(v in selected_values for v in lst))
        else:
            mask = df[column].isin(selected_values)
        final_mask &= mask

final_mask &= df['number_of_subjects'].between(subject_range[0], subject_range[1])
if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1])
    final_mask &= df['date_updated'].between(start_date, end_date, inclusive='both')

filtered_df = df[final_mask].copy()

# --- Main Panel ---
st.title("🔬 TCIA Dataset Explorer")
st.markdown("An interactive tool to filter and find datasets from The Cancer Imaging Archive.")
st.write(f"**Found {len(filtered_df)} matching datasets.**")

# --- Share Query ---
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

# --- Sorting and Pagination Controls ---
sort_col, order_col, size_col = st.columns([3, 2, 2])
sort_by = sort_col.selectbox("Sort by", options=["Release Date", "Alphabetical (Short Title)", "Number of Subjects"], index=0)
sort_order = order_col.radio("Order", options=["Descending", "Ascending"], horizontal=True)
PAGE_SIZE = size_col.selectbox("Results per page", options=[10, 25, 50, 100], index=1)

# Apply Sorting
ascending = (sort_order == "Ascending")
if sort_by == "Release Date":
    filtered_df = filtered_df.sort_values(by="date_updated", ascending=ascending)
elif sort_by == "Alphabetical (Short Title)":
    filtered_df['sort_title_tmp'] = filtered_df['short_title'].fillna('').replace('', np.nan).fillna(filtered_df['title']).str.lower()
    filtered_df = filtered_df.sort_values(by="sort_title_tmp", ascending=ascending)
elif sort_by == "Number of Subjects":
    filtered_df = filtered_df.sort_values(by="number_of_subjects", ascending=ascending)

# Pagination logic
if 'current_page' not in st.session_state: st.session_state.current_page = 1
total_results = len(filtered_df)
total_pages = max(1, (total_results // PAGE_SIZE) + (1 if total_results % PAGE_SIZE > 0 else 0))
st.session_state.current_page = min(st.session_state.current_page, total_pages)

def render_pagination(key_suffix):
    st.write(f"Page {st.session_state.current_page} of {total_pages}")
    prev_col, next_col = st.columns(2)
    if prev_col.button("⬅️ Previous", key=f"prev_{key_suffix}", use_container_width=True, disabled=(st.session_state.current_page <= 1)):
        st.session_state.current_page -= 1
        st.rerun()
    if next_col.button("Next ➡️", key=f"next_{key_suffix}", use_container_width=True, disabled=(st.session_state.current_page >= total_pages)):
        st.session_state.current_page += 1
        st.rerun()

if total_pages > 1:
    render_pagination("top")
    st.markdown("---")

start_index = (st.session_state.current_page - 1) * PAGE_SIZE
end_index = start_index + PAGE_SIZE
paginated_df = filtered_df.iloc[start_index:end_index]

if paginated_df.empty:
    st.warning("No datasets match the current filter criteria. Please broaden your search.")
else:
    for _, row in paginated_df.iterrows():
        display_title = f"{row.get('short_title', '')} | {row['title']}" if row.get('short_title') else row['title']
        st.markdown(f"### [{display_title}]({row['link']})")

        # Compact layout
        c1, c2, c3, c4, c5 = st.columns([3, 2, 1, 1, 1.5])
        doi_val = row.get('doi')
        if doi_val and str(doi_val) != 'nan' and str(doi_val) != '':
            c1.markdown(f"**DOI:** [{doi_val}](https://doi.org/{doi_val})")
        else:
            c1.markdown("**DOI:** N/A")

        c2.markdown(f"**Program:** {format_tags(row.get('program'))}")
        c3.markdown(f"**Type:** `{row['dataset_type']}`")
        c4.markdown(f"**Subjects:** `{row['number_of_subjects']}`")
        c5.markdown(f"**Updated:** `{row['date_updated'].strftime('%Y-%m-%d') if pd.notna(row['date_updated']) else 'N/A'}`")

        c6, c7 = st.columns(2)
        c6.markdown(f"**Data Type(s):** {format_tags(row.get('data_types'))}")
        c7.markdown(f"**External Resources:** {format_tags(row.get('supporting_data'))}")

        c8, c9 = st.columns(2)
        c8.markdown(f"**Data Category:** {format_tags(row.get('data_category'))}")
        c9.markdown(f"**License(s):** {format_tags(row.get('licenses'))}")

        st.markdown(f"**Cancer Type(s):** {format_tags(row.get('cancer_types'))}")
        st.markdown(f"**Cancer Location(s):** {format_tags(row.get('cancer_locations'))}")

        # Related Datasets - Rendering them directly to ensure links work
        related = row.get('related_datasets', [])
        if isinstance(related, (list, np.ndarray)) and len(related) > 0:
            st.markdown(f"**Related Datasets:** {' | '.join(related)}")
        else:
            st.markdown("**Related Datasets:** N/A")

        if row.get('citation') or row.get('summary'):
            with st.expander("View Citation and Abstract"):
                if row.get('citation'):
                    st.markdown(f"**Citation:** {row['citation']}")
                    if row.get('summary'): st.markdown("---")
                if row.get('summary'):
                    st.markdown(f"**Abstract:** {row['summary']}")

        # Link to downloads via parent_id
        dataset_id = row['id']
        relevant_downloads = downloads_df[downloads_df['parent_id'] == dataset_id]

        if not relevant_downloads.empty:
            with st.expander("View Downloadable Files"):
                    html = "<table style='width:100%'>"
                    html += "<tr><th>Title</th><th>Size</th><th>Category</th><th>Data Type</th><th>License</th><th>Links</th></tr>"
                    for _, d_row in relevant_downloads.iterrows():
                        links = []
                        if d_row['download_url']:
                            links.append(f'<a href="{d_row["download_url"]}" target="_blank">Download</a>')
                        if d_row['search_url']:
                            links.append(f'<a href="{d_row["search_url"]}" target="_blank">Search</a>')

                        links_html = " | ".join(links)
                        category = format_tags_html(d_row.get('download_types', []))
                        data_types = format_tags_html(d_row.get('data_types', []))
                        license_label = d_row['license_label']
                        size = f"{d_row['download_size']} {d_row['download_size_unit']}"

                        html += f"<tr><td>{d_row['download_title']}</td><td>{size}</td><td>{category}</td><td>{data_types}</td><td>{license_label}</td><td>{links_html}</td></tr>"
                    html += "</table>"
                    st.markdown(html, unsafe_allow_html=True)

        st.markdown("---")

    if total_pages > 1:
        render_pagination("bottom")
