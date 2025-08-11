# sync_data.py
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

def parse_string_to_list(value):
    if isinstance(value, list): return value
    if pd.isna(value) or value == '': return []
    try:
        parsed_list = ast.literal_eval(str(value))
        return parsed_list if isinstance(parsed_list, list) else []
    except (ValueError, SyntaxError):
        return []

def extract_renderer_title(series_val):
    if pd.isna(series_val): return ""
    try:
        parsed_dict = ast.literal_eval(str(series_val))
        return parsed_dict.get('rendered', str(series_val))
    except (ValueError, SyntaxError):
        return str(series_val)

def clean_list_of_dicts(lod):
    if not isinstance(lod, list):
        return []
    cleaned_list = []
    for d in lod:
        if not isinstance(d, dict):
            continue
        cleaned_dict = {}
        for k, v in d.items():
            if isinstance(v, list):
                cleaned_dict[k] = [None if pd.isna(item) else item for item in v]
            elif pd.isna(v):
                cleaned_dict[k] = None
            else:
                cleaned_dict[k] = v
        cleaned_list.append(cleaned_dict)
    return cleaned_list

def main():
    MASTER_DATA_FILE = "tcia_master_data.parquet"
    CITATION_CACHE_FILE = "citations_cache.parquet"
    print("--- Starting TCIA Data Sync ---")

    print("Fetching live data...")
    try:
        raw_collections_df = wordpress.getCollections(format='df')
        raw_analyses_df = wordpress.getAnalyses(format='df')
        raw_downloads_df = wordpress.getDownloads(format='df')
        datacite_df = datacite.getDoi()
        print("Successfully fetched all data from APIs.")
    except Exception as e:
        print(f"FATAL ERROR: Could not fetch data. Details: {e}")
        return

    print("Processing and standardizing API data...")
    collection_col_map = {
        'id': 'id', 'link': 'link', 'title': 'title', 'short_title': 'collection_short_title',
        'doi': 'collection_doi', 'date_updated': 'date_updated', 'number_of_subjects': 'subjects',
        'cancer_types': 'cancer_types', 'cancer_locations': 'cancer_locations',
        'supporting_data': 'supporting_data', 'data_types': 'data_types', 'program': 'program',
        'related_collection': 'related_collection', 'related_analysis_results': 'related_analysis_results',
        'access_type': 'collection_page_accessibility', 'downloads': 'collection_downloads'
    }
    analysis_col_map = {
        'id': 'id', 'link': 'link', 'title': 'title', 'short_title': 'result_short_title',
        'doi': 'result_doi', 'date_updated': 'date_updated', 'number_of_subjects': 'subjects',
        'cancer_types': 'cancer_types', 'cancer_locations': 'cancer_locations',
        'data_types': 'supporting_data', 'program': 'program',
        'related_collections': 'related_collections', 'related_analysis_results': 'related_analysis_results',
        'access_type': 'result_page_accessibility', 'downloads': 'result_downloads'
    }

    collections_df = pd.DataFrame([ {dest: row.get(src) for dest, src in collection_col_map.items()} for _, row in raw_collections_df.iterrows() ])
    collections_df['dataset_type'] = 'Collection'
    analyses_df = pd.DataFrame([ {dest: row.get(src) for dest, src in analysis_col_map.items()} for _, row in raw_analyses_df.iterrows() ])
    analyses_df['dataset_type'] = 'Analysis Result'
    analyses_df['supporting_data'] = [[] for _ in range(len(analyses_df))]

    master_df = pd.concat([collections_df, analyses_df], ignore_index=True)
    master_df['title'] = master_df['title'].apply(extract_renderer_title)
    master_df['date_updated'] = pd.to_datetime(master_df['date_updated'], errors='coerce')
    master_df['number_of_subjects'] = pd.to_numeric(master_df['number_of_subjects'], errors='coerce').fillna(0).astype(int)

    print("Merging DataCite abstracts...")
    doi_to_description_df = datacite_df[['DOI', 'Description']].copy()
    master_df['doi_lower'] = master_df['doi'].str.lower()
    doi_to_description_df['doi_lower'] = doi_to_description_df['DOI'].str.lower()
    master_df = pd.merge(master_df, doi_to_description_df[['doi_lower', 'Description']], on='doi_lower', how='left')
    master_df = master_df.rename(columns={'Description': 'summary'})

    print("Integrating download data...")
    master_df['downloads'] = master_df['downloads'].apply(parse_string_to_list)

    downloads_df = raw_downloads_df.set_index('id')
    download_cols_to_keep = [
        'download_requirements', 'download_size', 'download_title', 'data_license',
        'download_size_unit', 'download_type', 'download_url', 'search_url', 'subjects',
        'data_type', 'study_count', 'file_type', 'series_count', 'image_count', 'date_updated'
    ]
    for col in download_cols_to_keep:
        if col not in downloads_df.columns:
            downloads_df[col] = pd.NA

    def get_download_details(ids):
        if not ids: return []
        valid_ids = [int(i) for i in ids if str(i).isdigit()]
        if not valid_ids: return []
        records = downloads_df.loc[downloads_df.index.intersection(valid_ids), download_cols_to_keep]
        return records.to_dict('records')

    master_df['downloads_info'] = master_df['downloads'].apply(get_download_details)
    master_df['downloads_info'] = master_df['downloads_info'].apply(clean_list_of_dicts)
    print("Download data integration complete.")

    print("\nLoading existing citation cache.")
    if os.path.exists(CITATION_CACHE_FILE):
        citations_cache = pd.read_parquet(CITATION_CACHE_FILE)
    else:
        citations_cache = pd.DataFrame(columns=['doi', 'citation'])

    master_df = pd.merge(master_df, citations_cache, on='doi', how='left')
    master_df['citation'] = master_df['citation'].fillna('')
    needs_update_mask = master_df['citation'].str.contains("not found|Could not retrieve|No DOI|^$", na=True)
    rows_to_update = master_df[needs_update_mask]

    if not rows_to_update.empty:
        total_to_fetch = len(rows_to_update)
        print(f"Found {total_to_fetch} datasets needing a citation. Fetching now...")
        new_citations = {}
        for i, (index, row) in enumerate(rows_to_update.iterrows()):
            citation_text = get_apa_citation(row['doi'])
            new_citations[row['doi']] = citation_text
            print(f"  Fetching citation {i+1}/{total_to_fetch} for DOI: {row['doi']}")
            time.sleep(0.1)

        master_df['citation'] = master_df['doi'].map(new_citations).fillna(master_df['citation'])
        new_cache_df = pd.DataFrame(new_citations.items(), columns=['doi', 'citation'])
        updated_cache = pd.concat([citations_cache, new_cache_df]).drop_duplicates(subset='doi', keep='last')
        updated_cache.to_parquet(CITATION_CACHE_FILE, index=False)
        print("Incremental citation fetch complete. Cache updated.")
    else:
        print("No new citations to fetch.")

    print("\nCleaning data and finalizing master DataFrame...")
    for col in ['related_collection', 'related_collections', 'related_analysis_results']:
        if col not in master_df.columns: master_df[col] = [[] for _ in range(len(master_df))]
        master_df[col] = master_df[col].apply(parse_string_to_list)
    master_df['related_datasets'] = master_df['related_collection'] + master_df['related_collections'] + master_df['related_analysis_results']

    list_cols = ['cancer_types', 'cancer_locations', 'supporting_data', 'data_types', 'program', 'related_datasets', 'downloads_info']
    for col in master_df.columns:
        if col in list_cols:
            master_df[col] = master_df[col].apply(lambda x: x if isinstance(x, list) else [])
        else:
            master_df[col] = master_df[col].fillna('')

    final_cols = [
        'id', 'link', 'title', 'short_title', 'summary', 'dataset_type', 'citation', 'doi',
        'cancer_types', 'cancer_locations', 'supporting_data', 'data_types',
        'number_of_subjects', 'date_updated', 'program', 'related_datasets', 'access_type',
        'downloads_info'
    ]
    for col in final_cols:
        if col not in master_df.columns:
            master_df[col] = ''
            if col in list_cols:
                master_df[col] = [[] for _ in range(len(master_df))]

    master_df = master_df[final_cols]
    master_df.to_parquet(MASTER_DATA_FILE, index=False)
    print(f"\n--- SUCCESS: Data saved to {MASTER_DATA_FILE} ---")

if __name__ == "__main__":
    main()
