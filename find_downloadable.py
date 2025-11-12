
import pandas as pd

def find_dataset_with_downloads():
    master_df = pd.read_parquet("tcia_master_data.parquet")
    for index, row in master_df.iterrows():
        if any(row["collection_downloads"]) or any(row["result_downloads"]):
            print(f"Dataset with downloads found: {row['title']}")
            return

if __name__ == "__main__":
    find_dataset_with_downloads()
