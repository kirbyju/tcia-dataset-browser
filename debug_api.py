import pandas as pd
from tcia_utils import wordpress

print("--- Fetching sample download data ---")
try:
    ids = [43723, 43725, 43727]
    df = wordpress.getDownloads(ids=ids, format="df")

    if df is not None and not df.empty:
        print("\n--- DataFrame Info ---")
        df.info()

        print("\n--- DataFrame Content ---")
        # Use to_string() to print the full dataframe without truncation
        print(df.to_string())
    else:
        print("Failed to retrieve data or the dataframe is empty.")

except Exception as e:
    print(f"An error occurred: {e}")
