
import pandas as pd

def verify_data():
    """
    Verifies that the 'doi', 'summary', and 'citation' columns are correctly populated.
    """
    try:
        df = pd.read_parquet('tcia_master_data.parquet')

        # Check if the 'doi' column is not empty
        if 'doi' in df.columns and not df['doi'].isnull().all():
            print("'doi' column is correctly populated.")
        else:
            print("'doi' column is empty or not found.")

        # Check if the 'summary' column is not empty
        if 'summary' in df.columns and not df['summary'].isnull().all():
            print("'summary' column is correctly populated.")
        else:
            print("'summary' column is empty or not found.")

        # Check if the 'citation' column is not empty
        if 'citation' in df.columns and not df['citation'].isnull().all():
            print("'citation' column is correctly populated.")
        else:
            print("'citation' column is empty or not found.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    verify_data()
