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

## References

* Siriwardane, S., Sunderam, A., & Wallen, J. (2023). *Segmented Arbitrage*.
* Kelly, B., & Pruitt, S. (2013). “Market Expectations in the Cross-Section of Present Values.” *The Journal of Finance*, 68(4), 1721–1756.
* Fleckenstein, M., Longstaff, F., & Lustig, H. (2014). “Why Does the Treasury–TIPS Spread Predict Inflation?”
* Hazelkorn, D., van Binsbergen, J. H., & Grotteria, M. (2021). “Equity Spot–Futures Arbitrage.”
* Du, W., Hanson, S., & Soppe, A. (2018). “Covered Interest Parity Violations.”
* Fama, E. F., & French, K. R. (1993). “Common risk factors in the returns on stocks and bonds.”

```


