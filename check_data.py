
import pandas as pd

def check_downloads_data():
    try:
        df = pd.read_parquet('tcia_master_data.parquet')

        if 'downloads' in df.columns:
            # Check if any of the lists in the 'downloads' column are not empty
            if df['downloads'].apply(lambda x: isinstance(x, list) and len(x) > 0).any():
                print("Download data found in tcia_master_data.parquet.")
            else:
                print("No download data found in tcia_master_data.parquet.")
        else:
            print("'downloads' column not found in tcia_master_data.parquet.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    check_downloads_data()
