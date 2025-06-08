# import pytest
# import re
# import os
# from settings import config

# DATA_DIR = config("DATA_DIR")
# TABLES_DIR = config("TABLES_DIR")

# # List of LaTeX file names
# latex_files = [
#     "summary_table_6_monthly.tex",
#     "summary_table_6_annual.tex",
#     "summary_table_25_monthly.tex",
#     "summary_table_25_annual.tex",
#     "summary_table_100_monthly.tex",
#     "summary_table_100_annual.tex"
# ]

# # Define base directory for LaTeX files
# BASE_DIR = TABLES_DIR

# @pytest.mark.parametrize("filename", latex_files)
# def test_latex_table_presence(filename):
#     # Full file path
#     file_path = os.path.join(BASE_DIR, filename)
    
#     # Debugging: Print the resolved absolute path
#     print(f"Checking file: {os.path.abspath(file_path)}")

#     # Ensure file exists before proceeding
#     assert os.path.exists(file_path), f"File not found: {file_path}"

#     # Define keywords to check
#     keywords = ["R2 In-Sample", "R2 Out-of-Sample"]
    
#     # Define a regex pattern to detect floating point numbers
#     number_pattern = r"-?\d+\.\d+"

#     # Read the LaTeX file
#     with open(file_path, "r") as f:
#         content = f.read()

#     # Check if keywords exist
#     for keyword in keywords:
#         assert keyword in content, f"Keyword '{keyword}' not found in {filename}."

#     # Check if at least one floating point number exists
#     numbers_found = re.findall(number_pattern, content)
#     assert len(numbers_found) > 0, f"No numerical values found in {filename}."

# if __name__ == "__main__":
#     # Debugging: Print all files in the directory
#     print(f"Files in {os.path.abspath(BASE_DIR)}:")
#     print(os.listdir(BASE_DIR))

#     pytest.main()
