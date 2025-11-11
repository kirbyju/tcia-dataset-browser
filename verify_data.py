
import pandas as pd

def verify_data():
    """
    Verifies the contents of the generated parquet files.
    """
    try:
        # Verify the master data file
        master_df = pd.read_parquet('tcia_master_data.parquet')
        if 'related_datasets' in master_df.columns:
            # Check for the "ID: False" regression
            if master_df['related_datasets'].astype(str).str.contains("ID: False").any():
                print("Regression found in 'related_datasets' column.")
            else:
                print("'related_datasets' column is clean.")
        else:
            print("'related_datasets' column not found.")

        # Verify the downloads cache file
        downloads_df = pd.read_parquet('downloads_cache.parquet')
        if not downloads_df.empty:
            print("'downloads_cache.parquet' is correctly generated and not empty.")
        else:
            print("'downloads_cache.parquet' is empty.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    verify_data()
