#  Replication Suite

## Overview

This repository implements a high-performance replication of key empirical exercises from [Siriwardane, Sunderam, and Wallen’s “Segmented Arbitrage” (October 2023)]([url](https://www.nber.org/system/files/working_papers/w30561/w30561.pdf)) and [Kelly and Pruitt's "Market Expectations in the Cross-Section of Present Values" (2013)](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12060).  We string together parts of each of these papers, reproducing one arbitrage strategy from the paper, and integrate them into a unified, interactive front-end.  Our objectives are to:

1. **Replicate** TIPS–Treasury, equity spot-futures, Treasury spot-futures, cross-section present-value, and covered interest parity (CIP) arbitrage spreads over 2010–2020.
2. **Scale** data ingestion, processing, and analysis using distributed computing (Dask on AWS EMR / S3, Slurm on Midway3, MPI/Numba for compute-intensive kernels).
3. **Compare** synthetic (“paper”) yields to actual market yields and quantify mispricing dynamics.
5. **Demonstrate** how segmentation in funding and balance-sheet constraints drives cross-market heterogeneity in spreads.


## Replicated Papers 
### Market Expectations in the Cross-Section of Present Values [(Kelly and Pruitt, 2013)]([url](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12060)).
This paper demonstrates that returns and cash flows for the aggregate US stock market are highly predictable. It demonstrates this by constructing a univariate predictor from the cross section of stocks. See the abstract:
   '''
   Returns and cash flow growth for the aggregate U.S. stock market are highly and robustly predictable. Using a single factor extracted from the cross-section of book-to market ratios, we find an out-of-sample return forecasting R2 of 13% at the annual frequency (0.9% monthly). We document similar out-of-sample predictability for returns on value, size, momentum, and industry portfolios. We present a model linking aggregate market expectations to disaggregated valuation ratios in a latent factor system. Spreads in value portfolios’ exposures to economic shocks are key to identifying predictability and are consistent with duration-based theories of the value premium.
   '''
Data sources: CRSP and Compustat. Citation: [KELLY, B. and PRUITT, S. (2013), Market Expectations in the Cross-Section of Present Values. THE JOURNAL OF FINANCE, 68: 1721-1756. ]([url](https://doi.org/10.1111/jofi.12060)). [Serial reference](https://github.com/jaredszajkowski/finm32900_project_group6). 

### Segmented Arbitrage: FX Covered Interest Parity Spread
This trade is based off of the covered interest parity (CIP). A CIP deviation is a spread between a cash riskless rate and a synthetic riskless rate. The synthetic rate is local currency borrowing swapped into a foreign denominated rate using cross-currency derivatives. Data used: Bloomberg, maybe Datastream. [Serial reference](https://github.com/omehta101/CIP).

### Segmented Arbitrage: Equity Spot Futures
“We construct arbitrage implied forward rates using the nearby and first deferred futures contracts for the S&P 500, Dow Jones, and Nasdaq 100 indices.” Data used: Bloomberg. [Serial reference.](https://github.com/andyandikko/Equity_Spot_futures_arb)

### Segmented Arbitrage: Treasury Spot-Futures
Replicate the data pull and the construction of the Treasury-Spot Futures spread. The Treasury spot-futures arbitrage trade involves exploiting the price discrepancy between Treasury futures contracts and the underlying Treasury securities in the cash (spot) market. By simultaneously buying (or selling) the mispriced asset and selling (or buying) its equivalent in the other market, arbitrageurs aim to lock in a risk-free profit, accounting for carry costs, accrued interest, and delivery options. Data used: Bloomberg. [Serial reference.]([url](https://github.com/haoshuwang712123/FINM-32900-Final-Projcet-12)).

### Segmented Arbitrage: TIPS Treasury
“In the market for US sovereign debt, there is a no-arbitrage condition between inflation-swapped Treasury Inflation-Protected Securities (TIPS) and Treasuries (Fleckenstein et al., 2014). TIPS are US Treasury obligations for which the principal amount (and coupons) are adjusted for the Consumer Price Index (CPI). These inflation adjustments may be undone using an inflation swap, yielding fixed cash flows. The arbitrage spread is the difference in yield between this synthetic nominal Treasury constructed from TIPS and inflation swaps and a nominal Treasury.” Data sources: Bloomberg, and the Federal Reserve’s Yield Curve data ([zero coupon bond yields]( https://www.federalreserve.gov/data/yield-curve-tables/feds200628_1.html)). [Serial reference](https://github.com/BaileyMeche/TIPS_Treasury_Arbitrage).

## Social Science Motivation

### Research Questions

- **Persistence of Mispricing**: Do pricing discrepancies in large, liquid fixed-income and equity markets persist over 2010–2020, and how do they evolve?
- **Segmentation Drivers**: How do funding frictions (secured vs. unsecured funding costs) and balance-sheet segmentation among intermediaries explain low comovement in arbitrage spreads?
- **Macro-Micro Link**: Can cross-sectional predictors (e.g., book-to-market, dividend-price ratios) help explain time-series variation in aggregate spreads?

### Significance

Prior literature often assumes a representative intermediary with integrated funding and balance-sheet costs, implying near-perfect correlation in arbitrage spreads. Siriwardane et al. document a high-dimensional factor structure, pointing to **market segmentation**. By replicating and extending their analyses, we shed light on:

- The **enduring** nature of law-of-one-price violations even in post-crisis decades.
- The **role** of specific funding sources (e.g., TED spread shocks, money-market fund reforms) in driving strategy-specific spreads.
- The **impact** of specialized dealer and hedge-fund balance-sheet shocks (e.g., “London Whale”) on arbitrage opportunities.

These insights inform regulatory policy on market liquidity, systemic risk, and the design of macroprudential buffers. 

---

##  Scalable Computing

### Task-Runner & Scalable-Computing Architecture

| Layer                                   | What it Does                                                                                                                                                                                               | Why it Matters for Scale                                                                                           |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **1. PyDoit orchestration (`dodo.py`)** | • Each step (pull, clean, model, plot) is a *task* with `file_dep` & `targets`.  <br>• Tasks run **incrementally** (skip if outputs are up-to-date) and can be parallelised via `doit -n N`.               | Prevents wasted work and automatically creates a **DAG** of dependencies; trivial to re-run only what changed.     |
| **2. External compute hooks**           | • `task_start_dask` spins up a **Dask EC2 cluster** and writes `dask-scheduler.json`.  <br>• `task_start_scrapers` launches short-lived EC2 jobs (`scrapers_main.py`) that ingest raw data straight to S3. | Compute-heavy ETL or web-scraping is off-loaded to the cloud, freeing the laptop and avoiding local memory limits. |
| **3. Dask in analytics modules**        | • Each heavy ETL (e.g. `compute_tips_treasury.py`) reads 10M+ rows as **Dask DataFrames** and registers with the remote scheduler via <br>`scheduler = config("DASK_SCHEDULER_ADDRESS")`.                  | Lets us scale from a single core to dozens of workers without code changes—only the env-var flips.                 |
| **4. S3 as durable interchange**        | • Raw & intermediate Parquet/CSV land in S3 (`BUCKET/S3_MANUAL_DATA`).  <br>• Cluster workers pull/push directly; PyDoit only sees the final artifact in `_data/`.                                         | Keeps local repos lightweight and gives free “checkpointing” between independent job runs.                         |
| **5. Notebook automation**              | • Notebooks are executed headless (`nbconvert --execute`) by PyDoit; failures bubble up as task errors.  <br>• Clear-output + script-conversion tasks keep diffs reviewable.                               | We keep the *exploratory* experience of notebooks, **without** manual clicks, and still get reproducible builds.   |
| **6. Continuous clean-up**              | • `clean=True` on most tasks removes stale artifacts; `task_CIP_clean_reports` deletes LaTeX aux files.                                                                                                    | CI runners stay lean; no ghost files polluting future builds.                                                      |

1. **Data Volume & Velocity**

   * **Futures & OIS feeds:** tick-level prices ⇒ **gigabytes per month**.
   * **Ken French & CRSP panels:** thousands of equities × 100 yrs = tens of millions of rows.

2. **Compute Intensity**

   * **Forward-rate surface:** exponentials + root-finding on 4 tenors × 5 k trading days = 20 k expensive transforms, all vectorised in Dask.
   * **Recursive forecasts & bootstrap R²:** each strategy resamples >1 000 times ⇒ 30 000+ OLS fits; embarrassingly parallel.

3. **Pipeline Complexity**

   * **Fan-out DAG:** scraping → cleaning → merging → modelling → LaTeX → PDF/HTML.
   * Each node is its own Doit task *and* its own Dask/Spark job where warranted.

4. **Elastic Resources**

   * **EC2 t3.micro scrapers**: cheap fire-and-forget ingestion.
   * **Dask cluster**: spin up c5.xlarge workers only when `task_start_dask` runs; pay-as-you-go.
   * **Local fallback:** if `DASK_SCHEDULER_ADDRESS` is absent, code silently defaults to single-process execution—zero friction for small dry-runs.

5. **Reproducibility & Cost Control**

   * All infra coordinates through **one env file** (`settings.py` + `dask-scheduler.json`).
   * Kill the cluster → re-run Doit → tasks either reuse cached Parquet or relaunch workers— deterministic, idempotent, cheap.


## Large-Scale Computing Framework

### Cluster Environments

* **Parallel Scraping on EC2 → S3**
  The **`scrapers_main.py`** driver spins up a fleet of 3 EC2 scraper workers (`t3.micro`) via `launch_scraper_instances`. Each instance pulls one of your four scraper scripts (`scraper_1_fed_yield_curve.py`, `scraper_2_fed_tips_yield_curve.py`, `scraper_3_expectations.py`, etc.), writes its Parquet shard into `s3://<BUCKET>/raw/<module>/…`, and then terminates automatically once done.  The driver then waits (polling with `wait_for_termination`) before moving on.

* **Dask Cluster on EC2**
  We launch a dynamic Dask cluster using your **`cluster_setup.py`** module.  Under the hood, `EC2Cluster` from `dask_cloudprovider.aws` spins up 4 workers (4–8 `c7a.large`), each in a Docker image pre-pinned to your versions.  The scheduler address is written to `.env` and `dask-scheduler.json` so downstream notebooks and scripts can connect.  Once all workers register (via `Client.wait_for_workers()`), you can read your Parquet shards from S3 in parallel, run all of your ETL/compute pipelines (merges, `compute_tips_treasury()`, regressions, graph generation) via Dask DataFrame and `map_partitions`, then write out processed Parquet or bulk-load into your MySQL RDS.

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

## Outcomes 
All computational pipelines executed end-to-end and every target series from the five focal papers was successfully reproduced. 
| Paper / Strategy              | Target Metric                                                     | Our Output Artifact                                                | Status           |
| ----------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------ | ---------------- |
| Kelly-Pruitt (2013)           | Aggregate latent-factor predictor & out-of-sample $R^{2}$         | `_output/cross_section_factor.parquet`, `market_expectations.html` | **Replicated** ✅ |
| Covered-Interest Parity (CIP) | USD/JPY 3-month CIP basis                                         | `_output/cip_spread_plot_2025.png`                                 | **Replicated** ✅ |
| Equity Spot-Futures           | SPX, INDU, NDX implied forward curves & spreads                   | `_processed/all_indices_spread_to_present.png`                     | **Replicated** ✅ |
| Treasury Spot-Futures         | 2-, 5-, 10-year basis series                                      | `_data/treasury_sf_output.csv`                                     | **Replicated** ✅ |
| TIPS-Treasury                 | 2-, 5-, 10-, 20-year synthetic nominal yields & arbitrage spreads | `_output/tips_treasury_spreads.png`                                | **Replicated** ✅ |
This confirms the correctness of our scalable computing workflow.

## Additional Notes for Reproducibility & Optimization

- **Manual & Scraped Data Included**  
  All manual data (in `data_manual/`) and scraped shards (under `raw/` in S3 and mirrored in `_data/`) are provided in this pushed version to demonstrate the final output of every code path. Not all LaTeX‐based reports (in `reports/`) were precompiled; to generate them, run the LaTeX tasks in the Doit runner locally (e.g. `doit compile_latex_docs`).

- **Optimized Tasks with Dask on EC2**  
  Several modules take advantage of the Dask cluster spun up early in the Doit pipeline (via `task_start_dask`). These include the most compute‐heavy routines—such as `compute_tips_treasury.py`, cross‐section factor extraction, and bootstrap regressions—where parallel execution yields the greatest cost‐efficiency on EC2. By off‐loading these jobs to Dask EC2 workers, we minimize local resource usage and reduce wall‐clock time.

- **Doit Sensitivity & Conda Environment**  
  The Doit task runner is highly sensitive to package versions and Python dependencies. We strongly recommend building the Conda environment specified in `environment.yml`:
  ```bash
  conda env create -f environment.yml
  conda activate replication-suite
  ```
  This ensures exact package versions (e.g. Dask, boto3, PyYAML) so that all doit tasks execute without version conflicts.
- **Multiple Output Directories**
Doit requires each task to declare unique output paths. As a workaround, we maintain separate subdirectories under `_data/`, `_output/`, and `_processed/` for each module’s artifacts. If desired, you can customize these paths in settings.py or via your own environment file, but be sure that no two tasks share identical target filenames.
- **Scalability & Extensibility**
This framework is designed to scale horizontally. You can expand or parallelize additional modules—such as adding new scraped sources, launching more Dask workers, or distributing Monte Carlo simulations across more EC2 instances—simply by defining new Doit tasks and updating the task_start_dask parameters. The general pattern of “scrape → store Parquet in S3 → Dask‐based ETL → store processed data → generate outputs” can be extended indefinitely to leverage any additional AWS resources or cluster nodes.

## References 
Siriwardane, E., Sunderam, A., & Wallen, J. (2023). Segmented Arbitrage 

Fleckenstein, M., Longstaff, F., & Lustig, H. (2014). “Why Does the Treasury–TIPS Spread Predict Inflation?” American Economic Review

Hazelkorn, D., van Binsbergen, J. H., & Grotteria, M. (2021). “Equity Spot–Futures Arbitrage”

Du, W., Hanson, S., & Soppe, A. (2018). “Covered Interest Parity Violations and the Role of Collateral”

Fama, E. F., & French, K. R. (1993). “Common risk factors in the returns on stocks and bonds.”

[Kelly, Pruitt. (2013) "Market Expectations in the Cross-Section of Present Values" ](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12060)


## Quick Start

To quickest way to run code in this repo is to use the following steps. First, you must have the `conda`  
package manager installed (e.g., via Anaconda). However, I recommend using `mamba`, via [miniforge]
(https://github.com/conda-forge/miniforge) as it is faster and more lightweight than `conda`. Second, you 
must have TexLive (or another LaTeX distribution) installed on your computer and available in your path.
You can do this by downloading and 
installing it from here ([windows](https://tug.org/texlive/windows.html#install) 
and [mac](https://tug.org/mactex/mactex-download.html) installers).
Having done these things, open a terminal and navigate to the root directory of the project and create a 
conda environment using the following command:
```
conda create -n tips python=3.12
conda activate tips
```
and then install the dependencies with pip
```
pip install -r requirements.txt
```
Finally, you can then run 
```
doit
```
And that's it!

If you would also like to run the R code included in this project, you can either install
R and the required packages manually, or you can use the included `environment.yml` file.
To do this, run
```
mamba env create -f environment.yml
```
I'm using `mamba` here because `conda` is too slow. Activate the environment. 
Then, make sure to uncomment
out the RMarkdown task from the `dodo.py` file. Then,
run `doit` as before.

### Other commands

#### Unit Tests and Doc Tests

You can run the unit test, including doctests, with the following command:
```
pytest --doctest-modules
```
You can build the documentation with:
```
rm ./src/.pytest_cache/README.md 
jupyter-book build -W ./
```
Use `del` instead of rm on Windows

#### Setting Environment Variables

You can 
[export your environment variables](https://stackoverflow.com/questions/43267413/how-to-set-environment-variables-from-env-file) 
from your `.env` files like so, if you wish. This can be done easily in a Linux or Mac terminal with the following command:
```
set -a ## automatically export all variables
source .env
set +a
```
In Windows, this can be done with the included `set_env.bat` file,
```
set_env.bat
```

### General Directory Structure

 - The `assets` folder is used for things like hand-drawn figures or other
   pictures that were not generated from code. These things cannot be easily
   recreated if they are deleted.

 - The `_output` folder, on the other hand, contains dataframes and figures that are
   generated from code. The entire folder should be able to be deleted, because
   the code can be run again, which would again generate all of the contents.

 - The `data_manual` is for data that cannot be easily recreated. This data
   should be version controlled. Anything in the `_data` folder or in
   the `_output` folder should be able to be recreated by running the code
   and can safely be deleted.

 - I'm using the `doit` Python module as a task runner. It works like `make` and
   the associated `Makefile`s. To rerun the code, install `doit`
   (https://pydoit.org/) and execute the command `doit` from the `src`
   directory. Note that doit is very flexible and can be used to run code
   commands from the command prompt, thus making it suitable for projects that
   use scripts written in multiple different programming languages.

 - I'm using the `.env` file as a container for absolute paths that are private
   to each collaborator in the project. You can also use it for private
   credentials, if needed. It should not be tracked in Git.

### Data and Output Storage

I'll often use a separate folder for storing data. Any data in the data folder
can be deleted and recreated by rerunning the PyDoit command (the pulls are in
the dodo.py file). Any data that cannot be automatically recreated should be
stored in the "data_manual" folder. Because of the risk of manually-created data
getting changed or lost, I prefer to keep it under version control if I can.
Thus, data in the "_data" folder is excluded from Git (see the .gitignore file),
while the "data_manual" folder is tracked by Git.

Output is stored in the "_output" directory. This includes dataframes, charts, and
rendered notebooks. When the output is small enough, I'll keep this under
version control. I like this because I can keep track of how dataframes change as my
analysis progresses, for example.

Of course, the _data directory and _output directory can be kept elsewhere on the
machine. To make this easy, I always include the ability to customize these
locations by defining the path to these directories in environment variables,
which I intend to be defined in the `.env` file, though they can also simply be
defined on the command line or elsewhere. The `settings.py` is reponsible for
loading these environment variables and doing some like preprocessing on them.
The `settings.py` file is the entry point for all other scripts to these
definitions. That is, all code that references these variables and others are
loading by importing `config`.

#### Note on Data Dependencies
If your machine does not have Bloomberg access, it cannot run `xbbg` properly. When running `doit`, this will cause the task `pull_bloomberg_treasury_inflation_swaps` to fail with a `blpapi` error. To avoid this, run `doit` which creates the data directory `_output`, then manually insert the completed file `treasury_inflation_swaps.csv`.

### Naming Conventions

 - **`pull_` vs `load_`**: Files or functions that pull data from an external
 data source are prepended with "pull_", as in "pull_fred.py". Functions that
 load data that has been cached in the "_data" folder are prepended with "load_".
 For example, inside of the `pull_CRSP_Compustat.py` file there is both a
 `pull_compustat` function and a `load_compustat` function. The first pulls from
 the web, whereas the other loads cached data from the "_data" directory.


### Dependencies and Virtual Environments

#### Working with `pip` requirements

`conda` allows for a lot of flexibility, but can often be slow. `pip`, however, is fast for what it does.  You can install the requirements for this project using the `requirements.txt` file specified here. Do this with the following command:
```
pip install -r requirements.txt
```

The requirements file can be created like this:
```
pip list --format=freeze
```

#### Working with `conda` environments

The dependencies used in this environment (along with many other environments commonly used in data science) are stored in the conda environment called `tips` which is saved in the file called `environment.yml`. To create the environment from the file (as a prerequisite to loading the environment), use the following command:

```
conda env create -f environment.yml
```

Now, to load the environment, use

```
conda activate tips
```

Note that an environment file can be created with the following command:

```
conda env export > environment.yml
```

However, it's often preferable to create an environment file manually, as was done with the file in this project.

Also, these dependencies are also saved in `requirements.txt` for those that would rather use pip. Also, GitHub actions work better with pip, so it's nice to also have the dependencies listed here. This file is created with the following command:

```
pip freeze > requirements.txt
```

**Other helpful `conda` commands**

- Create conda environment from file: `conda env create -f environment.yml`
- Activate environment for this project: `conda activate tips`
- Remove conda environment: `conda remove --name tips --all`
- Create tips conda environment: `conda create --name myenv --no-default-packages`
- Create tips conda environment with different version of Python: `conda create --name myenv --no-default-packages python` Note that the addition of "python" will install the most up-to-date version of Python. Without this, it may use the system version of Python, which will likely have some packages installed already.

#### `mamba` and `conda` performance issues

Since `conda` has so many performance issues, it's recommended to use `mamba` instead. I recommend installing the `miniforge` distribution. See here: https://github.com/conda-forge/miniforge

