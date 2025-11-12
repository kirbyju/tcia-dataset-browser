# sync_data.py (Unified + Robust)
import pandas as pd
from tcia_utils import wordpress, datacite
import ast
import requests
import os

# --- Helper Functions ---
def get_apa_citation(doi):
    if not doi or pd.isna(doi):
        return "No DOI provided."
    url = f"https://citation.crosscite.org/format?doi={doi}&style=apa&lang=en-US"
    headers = {"Accept": "text/x-bibliography"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.text.strip().replace("https://doi.org/", "") if response.status_code == 200 else f"Citation not found for DOI: {doi}"
    except requests.RequestException:
        return f"Could not retrieve citation for DOI: {doi}"

def parse_api_list(value):
    if isinstance(value, list):
        return value
    if value is False or value == '' or (isinstance(value, float) and pd.isna(value)) or str(value).lower() == 'false':
        return []
    try:
        parsed = ast.literal_eval(str(value))
        return parsed if isinstance(parsed, list) else [parsed]
    except (ValueError, SyntaxError):
        return [value]

def normalize_wp_title(v):
    # WordPress commonly returns {'rendered': '...'}
    if isinstance(v, dict):
        return v.get('rendered', '')
    if v is None:
        return ''
    if isinstance(v, float) and pd.isna(v):
        return ''
    return str(v)

def coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, dict):
            s = normalize_wp_title(v)
            if s.strip():
                return s
        elif isinstance(v, str):
            if v.strip():
                return v
        else:
            s = str(v)
            if s.strip():
                return s
    return ''

def main():
    MASTER_DATA_FILE = "tcia_master_data.parquet"
    CITATION_CACHE_FILE = "citations_cache.parquet"
    DOWNLOADS_CACHE_FILE = "downloads_cache.parquet"

    print("--- Starting TCIA Data Sync ---")
    print("Fetching live data...")
    try:
        raw_collections_df = wordpress.getCollections(format='df')
        raw_analyses_df = wordpress.getAnalyses(format='df')
        datacite_df = datacite.getDoi()

        print("Fetching and caching downloads data...")
        fields = [
            "id", "date_updated", "download_title", "data_license", "download_access",
            "data_type", "file_type", "download_size", "download_size_unit", "subjects",
            "study_count", "series_count", "image_count", "download_type",
            "download_url", "search_url", "download_file"
        ]
        downloads_df = wordpress.getDownloads(format='df', fields=fields)

        # drop download_file column
        downloads_df = downloads_df.drop(columns=['download_file'])
        
        for col in ['download_size', 'data_license', 'download_type', 'subjects', 'study_count',
                    'series_count', 'image_count', 'data_type', 'file_type']:
            if col in downloads_df.columns:
                downloads_df[col] = downloads_df[col].astype(str)

        downloads_df.to_parquet(DOWNLOADS_CACHE_FILE, index=False)
        print(f"-> Successfully cached {len(downloads_df)} download records.")
        print("Successfully fetched all data from APIs.")
    except Exception as e:
        print(f"FATAL ERROR: Could not fetch data. Details: {e}")
        return

    print("Processing and standardizing API data...")

    # 1) Rename source columns → unified names
    collection_col_map = {
        'collection_title': 'title_src',
        'collection_short_title': 'short_title',
        'collection_doi': 'doi',
        'collection_page_accessibility': 'access_type',
    }
    analysis_col_map = {
        'result_title': 'title_src',
        'result_short_title': 'short_title',
        'result_doi': 'doi',
        'result_page_accessibility': 'access_type',
    }

    collections_df = raw_collections_df.rename(columns=collection_col_map).copy()
    analyses_df = raw_analyses_df.rename(columns=analysis_col_map).copy()

    collections_df['dataset_type'] = 'Collection'
    analyses_df['dataset_type'] = 'Analysis Result'

    # 2) Build robust 'title' for each frame:
    # Prefer explicit *_title fields (now in 'title_src'); fall back to WP core title.rendered, then browse title, then slug.
    if 'title' not in collections_df.columns:
        collections_df['title'] = ''
    collections_df['title'] = collections_df.apply(
        lambda r: coalesce(
            r.get('title_src', ''),
            normalize_wp_title(r.get('title', '')),
            r.get('collection_browse_title', ''),
            r.get('slug', '')
        ),
        axis=1
    )

    if 'title' not in analyses_df.columns:
        analyses_df['title'] = ''
    analyses_df['title'] = analyses_df.apply(
        lambda r: coalesce(
            r.get('title_src', ''),
            normalize_wp_title(r.get('title', '')),
            r.get('result_browse_title', ''),
            r.get('slug', '')
        ),
        axis=1
    )

    # 3) Ensure key columns exist and are typed
    for df in (collections_df, analyses_df):
        for col in ['short_title', 'access_type', 'doi', 'link']:
            if col not in df.columns:
                df[col] = ''
            df[col] = df[col].astype(str)

        # subjects → number_of_subjects
        if 'subjects' in df.columns:
            df['number_of_subjects'] = pd.to_numeric(df['subjects'], errors='coerce').fillna(0).astype(int)
        else:
            df['number_of_subjects'] = 0

        # dates
        df['date_updated'] = pd.to_datetime(df.get('date_updated', pd.NaT), errors='coerce')

    # 4) Concatenate
    master_df = pd.concat([collections_df, analyses_df], ignore_index=True)

    # 5) Normalize list-like columns
    list_cols = [
        'cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program',
        'related_collection', 'related_collections', 'related_analysis_results',
        'collection_downloads', 'result_downloads'
    ]
    for col in list_cols:
        if col in master_df.columns:
            master_df[col] = master_df[col].apply(parse_api_list)
        else:
            master_df[col] = [[] for _ in range(len(master_df))]

    # 6) Related datasets resolution (IDs -> titles)
    # Build ID -> Title map from finalized titles
    id_to_title_map = {}
    for _, row in master_df[['id', 'title']].iterrows():
        tid = str(row['id'])
        t = str(row['title']) if pd.notna(row['title']) else ''
        id_to_title_map[tid] = t if t else f"ID: {tid}"

    master_df['related_datasets_ids'] = master_df.apply(
        lambda row: parse_api_list(row.get('related_collection', [])) +
                    parse_api_list(row.get('related_collections', [])) +
                    parse_api_list(row.get('related_analysis_results', [])),
        axis=1
    )
    master_df['related_datasets'] = master_df['related_datasets_ids'].apply(
        lambda ids: sorted([id_to_title_map.get(str(i), f"ID: {i}") for i in ids])
    )

    # 7) Merge DataCite abstracts (summary)
    print("Merging DataCite abstracts...")
    datacite_df = datacite_df.copy()
    datacite_df.rename(columns={'DOI': 'doi'}, inplace=True)
    master_df['doi_lower'] = master_df['doi'].str.lower()
    datacite_df['doi_lower'] = datacite_df['doi'].str.lower()
    master_df = pd.merge(
        master_df,
        datacite_df[['doi_lower', 'Description']],
        on='doi_lower',
        how='left'
    ).rename(columns={'Description': 'summary'})

    # 8) Citations cache
    if os.path.exists(CITATION_CACHE_FILE):
        citations_cache = pd.read_parquet(CITATION_CACHE_FILE)
    else:
        citations_cache = pd.DataFrame(columns=['doi', 'citation'])

    master_df = pd.merge(master_df, citations_cache, on='doi', how='left')
    master_df['citation'] = master_df['citation'].fillna('')

    rows_to_update = master_df[
        master_df['citation'].str.contains("not found|Could not retrieve|No DOI|^$", na=True)
    ]

    if not rows_to_update.empty:
        print(f"Found {len(rows_to_update)} datasets needing a citation. Fetching now...")
        new_citations = {}
        for _, row in rows_to_update.iterrows():
            d = row['doi']
            if isinstance(d, str) and d.strip():
                new_citations[d] = get_apa_citation(d)
        if new_citations:
            master_df['citation'] = master_df['doi'].map(new_citations).fillna(master_df['citation'])
            new_cache_df = pd.DataFrame(new_citations.items(), columns=['doi', 'citation'])
            updated_cache = pd.concat([citations_cache, new_cache_df]).drop_duplicates(subset='doi', keep='last')
            updated_cache.to_parquet(CITATION_CACHE_FILE, index=False)
            print("Incremental citation fetch complete. Cache updated.")
        else:
            print("No valid DOIs to fetch citations for.")
    else:
        print("No new citations to fetch.")

    # 9) Final cleanup and save
    print("\nCleaning data and finalizing master DataFrame...")
    # Ensure strings where expected
    for col in ['title', 'short_title', 'summary', 'dataset_type', 'citation', 'doi', 'access_type', 'link']:
        if col not in master_df.columns:
            master_df[col] = ''
        master_df[col] = master_df[col].astype(str).fillna('')

    # Numeric/date types
    master_df['number_of_subjects'] = pd.to_numeric(master_df.get('number_of_subjects', 0), errors='coerce').fillna(0).astype(int)
    master_df['date_updated'] = pd.to_datetime(master_df.get('date_updated', pd.NaT), errors='coerce')

    final_cols = [
        'id', 'link', 'title', 'short_title', 'summary', 'dataset_type', 'citation', 'doi',
        'cancer_types', 'cancer_locations', 'supporting_data', 'data_types',
        'number_of_subjects', 'date_updated', 'program', 'related_datasets', 'access_type',
        'collection_downloads', 'result_downloads'
    ]

    # Ensure all final columns exist with appropriate defaults
    for col in final_cols:
        if col not in master_df.columns:
            if col in ['collection_downloads', 'result_downloads', 'related_datasets',
                       'cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program']:
                master_df[col] = [[] for _ in range(len(master_df))]
            elif col == 'date_updated':
                master_df[col] = pd.NaT
            elif col == 'number_of_subjects':
                master_df[col] = 0
            else:
                master_df[col] = ''

    master_df = master_df[final_cols]
    master_df.to_parquet(MASTER_DATA_FILE, index=False)
    print(f"\n--- SUCCESS: Data saved to {MASTER_DATA_FILE} ---")

if __name__ == "__main__":
    main()
