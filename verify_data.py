
import pandas as pd
import numpy as np

def verify_data():
    """
    Verifies the contents of the generated parquet files.
    """
    try:
        # Verify the master data file
        master_df = pd.read_parquet('tcia_master_data.parquet')

        # Check for non-empty columns
        for col in ['title', 'short_title', 'access_type', 'doi']:
            if col in master_df.columns and not master_df[col].isnull().all() and not (master_df[col] == '').all():
                print(f"'{col}' column is correctly populated.")
            else:
                print(f"'{col}' column is empty or not found.")

        # Check if the 'number_of_subjects' column contains a range of values
        if 'number_of_subjects' in master_df.columns:
            if master_df['number_of_subjects'].nunique() > 1:
                print("'number_of_subjects' column contains a range of values.")
            else:
                print("'number_of_subjects' column only contains a single value.")
        else:
            print("'number_of_subjects' column not found.")

        # Verify the downloads cache file and data types
        downloads_df = pd.read_parquet('downloads_cache.parquet')
        if not downloads_df.empty:
            print("'downloads_cache.parquet' is correctly generated and not empty.")

            # Check data types for merging
            if pd.api.types.is_numeric_dtype(pd.to_numeric(downloads_df['id'])):
                print("'downloads_df.id' column has a numeric data type.")
            else:
                print("'downloads_df.id' column does not have a numeric data type.")

            collection_downloads_flat = [item for sublist in master_df['collection_downloads'] for item in sublist]
            result_downloads_flat = [item for sublist in master_df['result_downloads'] for item in sublist]

            all_download_ids = pd.Series(collection_downloads_flat + result_downloads_flat)

            if all_download_ids.empty:
                print("No download IDs found in master_df, which may be expected.")
            elif pd.api.types.is_numeric_dtype(pd.to_numeric(all_download_ids)):
                 print("Download ID columns in master_df have a numeric data type.")
            else:
                print("Download ID columns in master_df are not compatible with numeric type.")

        else:
            print("'downloads_cache.parquet' is empty.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    verify_data()
