import pandas as pd
import requests
import os
import gzip
import json
import ast

# --- Configuration ---
DATASETS_URL = "https://github.com/kirbyju/tcia-query-skill/releases/download/tcia-snapshot-latest/agent_datasets.jsonl.gz"
DOWNLOADS_URL = "https://github.com/kirbyju/tcia-query-skill/releases/download/tcia-snapshot-latest/agent_current_downloads.jsonl.gz"
MASTER_DATA_FILE = "tcia_master_data.parquet"
DOWNLOADS_CACHE_FILE = "downloads_cache.parquet"
CITATION_CACHE_FILE = "citations_cache.parquet"

# --- Helper Functions ---
def get_apa_citation(doi):
    if not doi or pd.isna(doi) or doi == 'nan' or doi == '':
        return "No DOI provided."
    url = f"https://citation.crosscite.org/format?doi={doi}&style=apa&lang=en-US"
    headers = {"Accept": "text/x-bibliography"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.text.strip().replace("https://doi.org/", "") if response.status_code == 200 else f"Citation not found for DOI: {doi}"
    except requests.RequestException:
        return f"Could not retrieve citation for DOI: {doi}"

def parse_semicolon_list(value):
    if not value or pd.isna(value) or value == '':
        return []
    if isinstance(value, list):
        return value
    # Handle the case where the value might be a JSON-encoded list string (like in downloads)
    if str(value).startswith('['):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if v]
        except:
            pass
    return [v.strip() for v in str(value).split(';') if v.strip()]

def download_and_extract(url, filename):
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(filename + ".gz", 'wb') as f:
            f.write(response.content)
        print(f"Extracting {filename}.gz...")
        with gzip.open(filename + ".gz", 'rb') as f_in:
            with open(filename, 'wb') as f_out:
                f_out.write(f_in.read())
        os.remove(filename + ".gz")
    else:
        raise Exception(f"Failed to download {url}")

def main():
    print("--- Starting TCIA Data Sync from tcia-query-skill ---")

    try:
        download_and_extract(DATASETS_URL, "agent_datasets.jsonl")
        download_and_extract(DOWNLOADS_URL, "agent_current_downloads.jsonl")
    except Exception as e:
        print(f"FATAL ERROR: Could not fetch data. Details: {e}")
        return

    print("Loading JSONL data...")
    datasets_df = pd.read_json("agent_datasets.jsonl", lines=True)
    downloads_df = pd.read_json("agent_current_downloads.jsonl", lines=True)

    # Cleanup temporary files
    os.remove("agent_datasets.jsonl")
    os.remove("agent_current_downloads.jsonl")

    print(f"Loaded {len(datasets_df)} datasets and {len(downloads_df)} downloads.")

    # --- Process Downloads ---
    print("Processing downloads...")
    # Normalize list-like columns in downloads
    for col in ['download_types', 'external_resources', 'data_types', 'file_types']:
        if col in downloads_df.columns:
            downloads_df[col] = downloads_df[col].apply(parse_semicolon_list)

    # Ensure parent_id is present and of a consistent type
    if 'parent_id' not in downloads_df.columns:
        print("CRITICAL: 'parent_id' column missing in downloads data. Attempting to derive from raw_json...")
        def get_parent_id(raw):
            try: return json.loads(raw).get('parent_id')
            except: return None
        downloads_df['parent_id'] = downloads_df['raw_json'].apply(get_parent_id)

    # Save downloads cache
    downloads_df.to_parquet(DOWNLOADS_CACHE_FILE, index=False)
    print(f"-> Successfully cached {len(downloads_df)} download records.")

    # --- Process Datasets ---
    print("Processing datasets...")

    # Aggregate licenses and external resources from downloads to datasets
    downloads_agg = downloads_df.groupby('parent_id').agg({
        'license_label': lambda x: sorted(list(set(filter(None, x)))),
        'external_resources': lambda x: sorted(list(set([item for sublist in x for item in sublist if item])))
    }).reset_index().rename(columns={'parent_id': 'id', 'license_label': 'licenses_from_downloads', 'external_resources': 'external_resources_from_downloads'})

    master_df = pd.merge(datasets_df, downloads_agg, on='id', how='left')

    # We need to adapt these to the new model
    master_df['supporting_data'] = master_df['external_resources'].apply(parse_semicolon_list)
    # If external_resources was empty, try the aggregated one
    master_df['supporting_data'] = master_df.apply(lambda r: r['supporting_data'] if r['supporting_data'] else r.get('external_resources_from_downloads', []), axis=1)

    master_df['cancer_types'] = master_df['cancer_types'].apply(parse_semicolon_list)
    master_df['cancer_locations'] = master_df['cancer_locations'].apply(parse_semicolon_list)
    master_df['data_types'] = master_df['data_types'].apply(parse_semicolon_list)
    master_df['data_category'] = master_df['download_types'].apply(parse_semicolon_list)

    # Program is tricky, let's just use the program_name if it's there
    def extract_program_name(p):
        if not p or pd.isna(p): return []
        if 'program_name:' in str(p):
            return [str(p).split('program_name:')[1].split(';')[0].strip()]
        return [str(p).strip()]

    master_df['program'] = master_df['program'].apply(extract_program_name)

    # Licenses / Access mapping
    master_df['licenses_list'] = master_df['licenses_from_downloads'].fillna('').apply(lambda x: x if isinstance(x, list) else [])

    # DOI and dates
    master_df['doi'] = master_df['doi'].astype(str).replace('nan', '')
    master_df['date_updated'] = pd.to_datetime(master_df['date_updated'], errors='coerce')
    master_df['number_of_subjects'] = pd.to_numeric(master_df['subjects'], errors='coerce').fillna(0).astype(int)

    # Citation logic
    if os.path.exists(CITATION_CACHE_FILE):
        citations_cache = pd.read_parquet(CITATION_CACHE_FILE)
    else:
        citations_cache = pd.DataFrame(columns=['doi', 'citation'])

    master_df = pd.merge(master_df, citations_cache, on='doi', how='left')
    master_df['citation'] = master_df['citation'].fillna('')

    rows_to_update = master_df[
        (master_df['doi'] != '') &
        (master_df['citation'].str.contains("not found|Could not retrieve|No DOI|^$", na=True))
    ]

    if not rows_to_update.empty:
        print(f"Found {len(rows_to_update)} datasets needing a citation. Fetching now...")
        new_citations = {}
        for _, row in rows_to_update.iterrows():
            d = row['doi']
            if d and d != 'nan':
                new_citations[d] = get_apa_citation(d)
        if new_citations:
            # Map new citations back to master_df
            for doi, cite in new_citations.items():
                master_df.loc[master_df['doi'] == doi, 'citation'] = cite

            new_cache_df = pd.DataFrame(new_citations.items(), columns=['doi', 'citation'])
            updated_cache = pd.concat([citations_cache, new_cache_df]).drop_duplicates(subset='doi', keep='last')
            updated_cache.to_parquet(CITATION_CACHE_FILE, index=False)
            print("Incremental citation fetch complete. Cache updated.")

    # Related datasets resolution (IDs -> titles + DOI URLs)
    # Build a comprehensive lookup map indexed by ID, slug, short_title, short_title_key, and full title
    lookup_map = {}
    for _, row in datasets_df.iterrows():
        doi_val = str(row.get('doi', '')).strip()
        doi_url = None
        if doi_val and doi_val.lower() != 'nan' and doi_val != '':
            if doi_val.startswith('https://doi.org/'):
                doi_url = doi_val
            else:
                doi_url = f"https://doi.org/{doi_val}"

        info = {
            "title": str(row.get('title', '')).strip() if pd.notna(row.get('title')) else "",
            "short_title": str(row.get('short_title', '')).strip() if pd.notna(row.get('short_title')) else "",
            "doi_url": doi_url
        }

        keys_to_index = []
        if pd.notna(row.get('id')):
            keys_to_index.append(str(row['id']))
        if pd.notna(row.get('slug')) and str(row['slug']).strip():
            keys_to_index.append(str(row['slug']).strip())
            keys_to_index.append(str(row['slug']).strip().lower())
        if pd.notna(row.get('short_title')) and str(row['short_title']).strip():
            keys_to_index.append(str(row['short_title']).strip())
            keys_to_index.append(str(row['short_title']).strip().lower())
        if pd.notna(row.get('short_title_key')) and str(row['short_title_key']).strip():
            keys_to_index.append(str(row['short_title_key']).strip())
            keys_to_index.append(str(row['short_title_key']).strip().lower())
        if pd.notna(row.get('title')) and str(row['title']).strip():
            keys_to_index.append(str(row['title']).strip())
            keys_to_index.append(str(row['title']).strip().lower())

        for k in set(keys_to_index):
            if k:
                lookup_map[k] = info

    def resolve_related(raw_json_str):
        try:
            raw = json.loads(raw_json_str)
            resolved_links = []
            for field in ['related_collection', 'related_collections', 'related_analysis_results']:
                val = raw.get(field, [])
                if val is None:
                    continue
                if not isinstance(val, list):
                    val = [val]
                for item in val:
                    # Filter out placeholders like 0, False, '0', etc.
                    if not item or str(item) == 'False' or str(item) == '0':
                        continue
                    if isinstance(item, dict):
                        # Filter out dict placeholders like {'id': 0, 'title': '', 'url': False}
                        item_id = item.get('id')
                        if not item_id or str(item_id) == '0' or str(item_id) == 'False':
                            continue

                    target_id = None
                    target_title = None
                    target_url = None

                    if isinstance(item, dict):
                        target_id = str(item.get('id')) if item.get('id') is not None else None
                        target_title = item.get('title') or item.get('collection_title') or item.get('result_title')
                        # Only use DOI URLs per instructions. But if the item itself has url, we can check if it has a DOI.
                        item_url = item.get('url')
                        if item_url and ('doi.org' in str(item_url)):
                            target_url = item_url
                    else:
                        target_id = str(item)

                    # Now try to find a match in our lookup_map
                    matched_info = None
                    # Try lookup by target_id (and its lowercased version)
                    if target_id:
                        tid_clean = target_id.strip()
                        if tid_clean in lookup_map:
                            matched_info = lookup_map[tid_clean]
                        elif tid_clean.lower() in lookup_map:
                            matched_info = lookup_map[tid_clean.lower()]

                    # If not matched, and item is a dict, try lookup by its title/label fields
                    if not matched_info and isinstance(item, dict):
                        for title_key in ['title', 'collection_title', 'result_title']:
                            t_val = item.get(title_key)
                            if t_val:
                                t_clean = str(t_val).strip()
                                if t_clean in lookup_map:
                                    matched_info = lookup_map[t_clean]
                                    break
                                elif t_clean.lower() in lookup_map:
                                    matched_info = lookup_map[t_clean.lower()]
                                    break

                    # If we found matched_info, we use its title and doi_url
                    if matched_info:
                        if matched_info['title']:
                            target_title = matched_info['title']
                        elif matched_info['short_title']:
                            target_title = matched_info['short_title']

                        if matched_info['doi_url']:
                            target_url = matched_info['doi_url']

                    # Rely strictly on the DOI URL as requested. If we have target_url (and it's a DOI URL), format as Markdown link.
                    if target_title and target_url and ('doi.org' in str(target_url)):
                        resolved_links.append(f"[{target_title}]({target_url})")
                    elif target_title:
                        resolved_links.append(target_title)
                    elif target_id:
                        if target_id.isdigit():
                            resolved_links.append(f"ID: {target_id}")
                        else:
                            resolved_links.append(target_id)

            return sorted(list(set(resolved_links)))
        except Exception as e:
            return []

    master_df['related_datasets'] = master_df['raw_json'].apply(resolve_related)

    # Final selection and renaming to match expected format for app.py
    final_cols_map = {
        'id': 'id',
        'link': 'link',
        'title': 'title',
        'short_title': 'short_title',
        'summary': 'summary',
        'dataset_type': 'dataset_type',
        'citation': 'citation',
        'doi': 'doi',
        'cancer_types': 'cancer_types',
        'cancer_locations': 'cancer_locations',
        'supporting_data': 'supporting_data',
        'data_types': 'data_types',
        'data_category': 'data_category',
        'number_of_subjects': 'number_of_subjects',
        'date_updated': 'date_updated',
        'program': 'program',
        'licenses_list': 'licenses',
        'related_datasets': 'related_datasets'
    }

    master_df = master_df[list(final_cols_map.keys())].rename(columns=final_cols_map)
    master_df['collection_downloads'] = [[] for _ in range(len(master_df))]
    master_df['result_downloads'] = [[] for _ in range(len(master_df))]
    master_df['access_type'] = ''

    master_df.to_parquet(MASTER_DATA_FILE, index=False)
    print(f"\n--- SUCCESS: Data saved to {MASTER_DATA_FILE} ---")

if __name__ == "__main__":
    main()
