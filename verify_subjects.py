
import pandas as pd

def verify_subject_counts():
    """
    Verifies that the 'number_of_subjects' column contains a range of values.
    """
    try:
        df = pd.read_parquet('tcia_master_data.parquet')
        if 'number_of_subjects' in df.columns:
            # Check if the column contains more than just zeros
            if df['number_of_subjects'].nunique() > 1:
                print("'number_of_subjects' column contains a range of values.")
            else:
                print("'number_of_subjects' column only contains a single value.")
        else:
            print("'number_of_subjects' column not found.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    verify_subject_counts()
