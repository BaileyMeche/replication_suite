"""
Saves CIP Plot in output
"""

from pathlib import Path
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

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



# Setup paths
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

sns.set()

# Load data
df = pull_bloomberg_cip_data.load_raw(end='2020-01-01', plot=True)
df_2025 = pull_bloomberg_cip_data.load_raw(end='2025-01-01', plot=True)




# Save figure
filename = OUTPUT_DIR / 'CIP_Replication.png'


print(f"Plot saved at {filename}")

# Show plot
plt.show()
