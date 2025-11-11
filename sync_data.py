# sync_data.py (Corrected and Refactored)
import pandas as pd
from tcia_utils import wordpress, datacite
import ast
import requests
import time
import os

# --- Helper Functions ---
def get_apa_citation(doi):
    if not doi or pd.isna(doi): return "No DOI provided."
    url = f"https://citation.crosscite.org/format?doi={doi}&style=apa&lang=en-US"
    headers = {"Accept": "text/x-bibliography"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.text.strip().replace("https://doi.org/", "") if response.status_code == 200 else f"Citation not found for DOI: {doi}"
    except requests.RequestException:
        return f"Could not retrieve citation for DOI: {doi}"

def parse_api_list(value):
    if isinstance(value, list): return value
    if value is False or pd.isna(value) or value == '' or str(value).lower() == 'false': return []
    try:
        parsed = ast.literal_eval(str(value))
        return parsed if isinstance(parsed, list) else [parsed]
    except (ValueError, SyntaxError):
        return [value]

def extract_renderer_title(series_val):
    if pd.isna(series_val) or not isinstance(series_val, dict): return series_val
    return series_val.get('rendered', series_val)

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
            "download_url", "search_url"
        ]
        downloads_df = wordpress.getDownloads(format='df', fields=fields)

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

    id_to_title_map = {}
    for _, row in raw_collections_df.iterrows():
        title = row.get('short_title') or row.get('title', {}).get('rendered', f"ID: {row['id']}")
        id_to_title_map[str(row['id'])] = title
    for _, row in raw_analyses_df.iterrows():
        # Note: raw_analyses_df does not have a 'short_title' column
        title = row.get('title', {}).get('rendered', f"ID: {row['id']}")
        id_to_title_map[str(row['id'])] = title

    collection_col_map = {
        'collection_doi': 'doi',
        'subjects': 'number_of_subjects',
        'short_title': 'short_title',
        'access_type': 'access_type'
    }
    analysis_col_map = {
        'result_doi': 'doi',
        'subjects': 'number_of_subjects',
        'short_title': 'short_title',
        'access_type': 'access_type'
    }

    collections_df = raw_collections_df.rename(columns=collection_col_map)
    collections_df['dataset_type'] = 'Collection'

    analyses_df = raw_analyses_df.rename(columns=analysis_col_map)
    analyses_df['dataset_type'] = 'Analysis Result'

    master_df = pd.concat([collections_df, analyses_df], ignore_index=True)

    master_df['title'] = master_df['title'].apply(extract_renderer_title)
    master_df['date_updated'] = pd.to_datetime(master_df['date_updated'], errors='coerce')
    master_df['number_of_subjects'] = pd.to_numeric(master_df['number_of_subjects'], errors='coerce').fillna(0).astype(int)

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

    master_df['related_datasets_ids'] = master_df.apply(lambda row: row.get('related_collection', []) + row.get('related_collections', []) + row.get('related_analysis_results', []), axis=1)
    master_df['related_datasets'] = master_df['related_datasets_ids'].apply(
        lambda ids: sorted([id_to_title_map.get(str(id), f"ID: {id}") for id in ids])
    )

    print("Merging DataCite abstracts...")
    datacite_df.rename(columns={'DOI': 'doi'}, inplace=True)
    master_df['doi_lower'] = master_df['doi'].str.lower()
    datacite_df['doi_lower'] = datacite_df['doi'].str.lower()
    master_df = pd.merge(master_df, datacite_df[['doi_lower', 'Description']], on='doi_lower', how='left').rename(columns={'Description': 'summary'})

    if os.path.exists(CITATION_CACHE_FILE):
        citations_cache = pd.read_parquet(CITATION_CACHE_FILE)
    else:
        citations_cache = pd.DataFrame(columns=['doi', 'citation'])

    master_df = pd.merge(master_df, citations_cache, on='doi', how='left')
    master_df['citation'] = master_df['citation'].fillna('')
    rows_to_update = master_df[master_df['citation'].str.contains("not found|Could not retrieve|No DOI|^$", na=True)]

    if not rows_to_update.empty:
        print(f"Found {len(rows_to_update)} datasets needing a citation. Fetching now...")
        new_citations = {row['doi']: get_apa_citation(row['doi']) for _, row in rows_to_update.iterrows()}
        master_df['citation'] = master_df['doi'].map(new_citations).fillna(master_df['citation'])
        new_cache_df = pd.DataFrame(new_citations.items(), columns=['doi', 'citation'])
        updated_cache = pd.concat([citations_cache, new_cache_df]).drop_duplicates(subset='doi', keep='last')
        updated_cache.to_parquet(CITATION_CACHE_FILE, index=False)
        print("Incremental citation fetch complete. Cache updated.")
    else:
        print("No new citations to fetch.")

    print("\nCleaning data and finalizing master DataFrame...")
    master_df.fillna('', inplace=True)
    final_cols = [
        'id', 'link', 'title', 'short_title', 'summary', 'dataset_type', 'citation', 'doi',
        'cancer_types', 'cancer_locations', 'supporting_data', 'data_types',
        'number_of_subjects', 'date_updated', 'program', 'related_datasets', 'access_type',
        'collection_downloads', 'result_downloads'
    ]
    # Ensure all final columns exist, adding empty ones if necessary
    for col in final_cols:
        if col not in master_df.columns:
            master_df[col] = '' if col not in ['collection_downloads', 'result_downloads', 'related_datasets'] else [[] for _ in range(len(master_df))]

    master_df = master_df[final_cols]
    master_df.to_parquet(MASTER_DATA_FILE, index=False)
    print(f"\n--- SUCCESS: Data saved to {MASTER_DATA_FILE} ---")

if __name__ == "__main__":
    main()
