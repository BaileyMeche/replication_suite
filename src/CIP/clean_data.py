"""
Cleans the raw data pulled from Github and pushes it into a csv
"""

# double check this works
from pathlib import Path
try:
    from pull_bloomberg_cip_data import *
    import pull_bloomberg_cip_data as pull_bloomberg_cip_data
except ModuleNotFoundError:
    from src.pull_bloomberg_cip_data import *
    import src.pull_bloomberg_cip_data as pull_bloomberg_cip_data

try:
    from settings import config
except ModuleNotFoundError:
    from src.settings import config

DATA_DIR = Path(config("DATA_DIR"))  # Should point to '_data'

df = pull_bloomberg_cip_data.load_raw('2025-03-01')
output_file = DATA_DIR / "tidy_data.csv"

df.to_csv(output_file, index=False)

print(f"Cleaned data saved to {output_file}")