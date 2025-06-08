"""
Converts CIP data to hmtl and png
"""




import sys
import os
import pandas as pd
import numpy as np
import argparse  # ✅ Fix: Handle command-line arguments
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

try:
    from settings import config
except ModuleNotFoundError:
    from src.settings import config

try:
    from cip_analysis import compute_cip_statistics
    import cip_analysis as cip_stats
except ModuleNotFoundError:
    from src.cip_analysis import compute_cip_statistics
    import src.cip_analysis as cip_stats

try:
    from pull_bloomberg_cip_data import compute_cip
    import pull_bloomberg_cip_data as cip
except ModuleNotFoundError:
    from src.pull_bloomberg_cip_data import compute_cip
    import src.pull_bloomberg_cip_data as cip





cip_table_2025 = cip.compute_cip(end = '2025-01-01')
stats_dict = cip_stats.compute_cip_statistics(cip_table_2025)



OUTPUT_DIR = config("OUTPUT_DIR")


def html_to_png(html_file, png_file):
    """Convert an HTML file to PNG using Selenium and Chrome."""

    # Set up Chrome in headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Load the HTML file
    driver.get(f"file://{html_file}")

    # Take screenshot and save as PNG
    driver.save_screenshot(png_file)

    driver.quit()
    return png_file



def save_cip_statistics_as_html(stats_dict):
    output_dir = os.path.join(OUTPUT_DIR, "html_files")
    """Save CIP statistics as HTML files."""
    os.makedirs(output_dir, exist_ok=True)

    overall_html = os.path.join(output_dir, "cip_summary_overall.html")
    corr_html = os.path.join(output_dir, "cip_correlation_matrix.html")
    annual_html = os.path.join(output_dir, "cip_annual_statistics.html")

    stats_dict["overall_statistics"].to_html(overall_html)
    stats_dict["correlation_matrix"].to_html(corr_html)
    stats_dict["annual_statistics"].to_html(annual_html)

    return overall_html, corr_html, annual_html


def convert_html_to_png(html_files):
    """Convert HTML files to PNG images."""

    output_dir = os.path.join(OUTPUT_DIR, "main_cip_files")
    for html_file in html_files:
        png_file = os.path.join(output_dir, os.path.basename(html_file).replace(".html", ".png"))
        html_to_png(html_file, png_file)


html_files = save_cip_statistics_as_html(stats_dict)
convert_html_to_png(html_files)