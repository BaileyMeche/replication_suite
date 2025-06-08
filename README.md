# Replication Suite

## Overview

High-performance replication of key empirical exercises from:

- **Segmented Arbitrage** (Siriwardane, Sunderam & Wallen, 2023)  
- **Market Expectations in the Cross-Section of Present Values** (Kelly & Pruitt, 2013)

We reproduce five core strategies—TIPS–Treasury, equity spot-futures, Treasury spot-futures, cross-section present-value, covered interest parity (CIP)—over 2010–2020, and provide a scalable “scrape → ETL → analyze → visualize” pipeline using Dask and AWS.

## Paper Modules

1. **Market Expectations**  
   - Latent-factor predictors from book-to-market ratios  
   - Data: CRSP & Compustat

2. **FX Covered Interest Parity (CIP)**  
   - USD/JPY 3m CIP basis  
   - Data: Bloomberg forwards, LIBOR

3. **Equity Spot-Futures**  
   - Implied forwards for SPX, INDU, NDX  
   - Data: Bloomberg futures

4. **Treasury Spot-Futures**  
   - Basis between Treasury futures & spot  
   - Data: Bloomberg CME

5. **TIPS–Treasury**  
   - Synthetic nominals from TIPS + inflation swaps vs. zero-coupon Treasuries  
   - Data: Bloomberg & FRB Yield Curve

## Motivation

- **Persistence of Mispricing**  
- **Segmentation Drivers** (funding frictions, balance-sheet constraints)  
- **Macro–Micro Link** via cross-sectional predictors  

These insights inform market‐liquidity policy and systemic‐risk regulation.

## Architecture

### 1. Parallel Scraping → S3  
- The `src/scrapers/` directory contains scraper scripts (`scraper_1_…`, `scraper_2_…`, `scraper_3_…`) orchestrated by **`scrapers_main.py`**, which launches AWS EC2 workers (`t3.micro`) via `launch_scraper_instances`. Each worker pulls data in parallel and writes Parquet shards to `s3://<BUCKET>/raw/<module>/`.

### 2. Clustered ETL & Analysis  
- **Dask EC2Cluster** (via `cluster_setup.py`) spins up 4–8 `c7a.large` workers on AWS.  
- **Parallel compute modules**:  
  - `src/expectations/run_regressions.py` runs cross-section regressions in parallel across Dask workers.  
  - `src/tips_treasury/compute_tips_treasury.py` computes TIPS–Treasury spreads in parallel via Dask DataFrames.  
- All shards are read from S3, processed, and written back as Parquet under `s3://<BUCKET>/processed/`.

### 3. Visualization & Reports  
- Headless notebook execution (`nbconvert`) and LaTeX compilation via PyDoit (`dodo.py`).  
- Outputs (plots, tables, HTML) land under `_output/`; intermediate data in `_data/`.

### 4. Local GPU Fallback  
- If no Dask scheduler is set, code defaults to single-process execution.  
- Optionally, connect a local NVIDIA T4 node for GPU-accelerated plotting or CuDF transforms.


## Repository Structure

```scss
final-project-baileymeche/
├── dodo.py                      ← all PyDoit tasks
├── requirements.txt             ← local dev requirements
├── settings.py                  ← Decouple config (DATA_DIR, …)
├── dask-scheduler.json          ← touched by start_dask task
│
├── src/                         ← project source code
│   ├── CIP/
│   │   ├── main_cip.ipynb
│   │   ├── cip_analysis.py
│   │   ├── pull_bloomberg_cip_data.py
│   │   ├── directory_functions.py
│   │   └── clean_data.py
│   │
│   ├── equity_spot/
│   │   ├── pull_bloomberg_data.py
│   │   ├── futures_data_processing.py
│   │   ├── OIS_data_processing.py
│   │   ├── Spread_calculations.py
│   │   ├── 01_OIS_Data_Processing.ipynb
│   │   ├── 02_Futures_Data_Processing.ipynb
│   │   └── 03_Spread_Calculations.ipynb
│   │
│   ├── expectations/
│   │   ├── pull_ken_french_data.py
│   │   ├── pull_CRSP_index.py
│   │   ├── regressions.py
│   │   ├── 01_Market_Expectations_In_The_Cross-Section_Of_Present_Values_Final.ipynb
│   │   └── run_regressions.ipynb
│   │
│   ├── tips_treasury/
│   │   ├── compute_tips_treasury.py
│   │   ├── pull_fed_yield_curve.py
│   │   ├── pull_fed_tips_yield_curve.py
│   │   ├── pull_bloomberg_treasury_inflation_swaps.py
│   │   ├── generate_figures.py
│   │   └── generate_latex_table.py
│   │
│   ├── treasury_spot/
│   │   ├── calc_treasury_data.py
│   │   ├── clean_raw.ipynb
│   │   ├── generate_reference.py
│   │   ├── load_bases_data.py
│   │   ├── process_treasury_data.ipynb
│   │   ├── 01_explore_basis_trade_data_new.ipynb
│   │   ├── test_calc_treasury_data.py
│   │   └── test_load_bases_data.py
│   │
│   └── scrapers/
│       ├── scrapers_main.py
│       ├── launch_scraper_instances.py
│       ├── s3_utils.py
│       ├── requirements.txt
│       ├── scraper_1_fed_yield_curve.py
│       ├── scraper_2_fed_tips_yield_curve.py
│       └── scraper_3_expectations.py
│
├── data_manual/                 ← hand-downloaded / scraped raw files
│   └── CIP_2025.xlsx
│
├── _data/                       ← **DATA_DIR**  (clean & intermediate datasets)
│   ├── fed_yield_curve.parquet
│   ├── fed_tips_yield_curve.parquet
│   ├── treasury_df.csv
│   └── …                         (etc.)
│
├── _output/                     ← **OUTPUT_DIR**  (plots, tables, html, tex)
│   ├── html_files/
│   │   └── main_cip.html
│   ├── main_cip_files/
│   │   ├── cip_spread_plot_replication.png
│   │   └── cip_spread_plot_2025.png
│   └── …                         (figures, LaTeX tables, notebooks copies)
│
├── _processed/                  ← processed futures / OIS csvs
│   ├── SPX_calendar_spread.csv
│   └── all_indices_calendar_spreads.csv
│
├── reports/                     ← LaTeX sources & compiled PDFs
│   ├── report.tex
│   ├── report.pdf
│   └── project.tex / project.pdf
│
├── docs_src/                    ← MkDocs / Sphinx source (if enabled)
└── _docs/                       ← built documentation and notebook copies
```

## Quickstart

1. **Clone & configure**  
   ```bash
   git clone ...
   cd replication-suite
   cp settings.example.py settings.py
````

2. **Create environment**

   ```bash
   conda env create -f environment.yml
   conda activate replication-suite
   ```

3. **Run pipeline**

   ```bash
   doit
   ```

4. **View outputs**

   * `_data/` for cached datasets
   * `_output/` for figures, HTML, PDF reports

## Notes

* All raw and manual data are included.
* To regenerate LaTeX reports, run `doit compile_latex_docs`.
* Use the provided Conda spec to avoid version conflicts.
* Unique output directories under `_data/`, `_processed/`, `_output/` prevent collisions.
* Easily scale by adding EC2 scrapers or Dask workers in Doit tasks.

## References

* Siriwardane, S., Sunderam, A., & Wallen, J. (2023). *Segmented Arbitrage*.
* Kelly, B., & Pruitt, S. (2013). “Market Expectations in the Cross-Section of Present Values.” *The Journal of Finance*, 68(4), 1721–1756.
* Fleckenstein, M., Longstaff, F., & Lustig, H. (2014). “Why Does the Treasury–TIPS Spread Predict Inflation?”
* Hazelkorn, D., van Binsbergen, J. H., & Grotteria, M. (2021). “Equity Spot–Futures Arbitrage.”
* Du, W., Hanson, S., & Soppe, A. (2018). “Covered Interest Parity Violations.”
* Fama, E. F., & French, K. R. (1993). “Common risk factors in the returns on stocks and bonds.”

```


