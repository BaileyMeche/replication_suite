import os
import pandas as pd
from settings import config
import load_bases_data  # Ensure this module is correctly located in src/

# Get DATA_DIR from config
DATA_DIR = config("DATA_DIR")

# Ensure DATA_DIR exists
os.makedirs(DATA_DIR, exist_ok=True)

# Load the dataset
df = load_bases_data.load_combined_spreads_wide(data_dir=DATA_DIR)

# Forward-fill missing values (limit 5), then drop remaining NaNs
df = df.ffill(limit=5).dropna()

# Reindex columns in sorted order and filter only columns matching "Treasury_SF_*"
filtered_df = df.reindex(sorted(df.columns), axis=1).filter(regex="^Treasury_SF_")

# Define output file path
output_file = os.path.join(DATA_DIR, "reference.csv")

# Save the filtered DataFrame to CSV in DATA_DIR
filtered_df.to_csv(output_file, index=True)

# Display dataset information
print(filtered_df.info())

print(f"Reference dataset saved to {output_file}")
