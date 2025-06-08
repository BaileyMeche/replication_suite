import os 
import sys
import shutil
from os import environ, getcwd, path
from pathlib import Path
from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter
from settings import config

sys.path.insert(1, "./src/")
sys.path.insert(1, "./src/tips_treasury/")
sys.path.insert(1, "./src/treasury_spot/")
sys.path.insert(1, "./src/CIP/")
sys.path.insert(1, "./src/expectations/")
sys.path.insert(1, "./src/equity_spot/")

sys.path.append(os.path.abspath("src"))  # Ensure `src/` is in the path

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        # other config here...
        # "cleanforget": True, # Doit will forget about tasks that have been cleaned.
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}
init(autoreset=True)


BASE_DIR                = Path(config("LOCAL_BASE_DIR"))
DATA_DIR                = Path(config("DATA_DIR"))
MANUAL_DATA_DIR         = Path(config("LOCAL_MANUAL_DATA_DIR"))
OUTPUT_DIR              = Path(config("OUTPUT_DIR"))
OUTPUT_DIR2              = Path(config("LOCAL_OUTPUT_DIR2"))
OUTPUT_DIR3              = Path(config("LOCAL_OUTPUT_DIR3"))
OUTPUT_DIR4              = Path(config("LOCAL_OUTPUT_DIR4"))
OS_TYPE                 = Path(config("OS_TYPE"))
PUBLISH_DIR             = Path(config("LOCAL_PUBLISH_DIR"))
USER                    = Path(config("USER"))
TEMP_DIR                = Path(config("LOCAL_TEMP_DIR"))
INPUT_DIR               = Path(config("INPUT_DIR"))
PROCESSED_DIR           = Path(config("PROCESSED_DIR"))
PLOTS_DIR               = Path(config("PLOTS_DIR"))
TABLES_DIR              = Path(config("TABLES_DIR"))

'''
import boto3
import time
from io import BytesIO
from pathlib import Path
from settings import config
s3 = boto3.client('s3')

# if you really want a Path object representing "bucket/prefix"
BUCKET_PATH = Path(config("BUCKET")) / config("S3_DATA")

# but for boto3 you typically need plain strings:
BUCKET   = config("BUCKET")      # e.g. "my-data-bucket"
S3_DATA  = config("S3_DATA")     # e.g. "tips_treasury/"

def s3_parquet(bucket, key):

    response = s3.get_object(Bucket=bucket, Key=key)
    file_stream = BytesIO(response['Body'].read())
    df = pd.read_parquet(file_stream, engine='fastparquet')  # or 'fastparquet'
    return df
'''

## Helpers for handling Jupyter Notebook tasks
# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
# def jupyter_execute_notebook(notebook):
#     return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --log-level WARN --inplace ./src/{notebook}.ipynb"
def jupyter_execute_notebook(nb_path_without_ext, allow_errors: bool = False):
    nb_file = Path("src") / f"{nb_path_without_ext}.ipynb"
    return (
           f'jupyter nbconvert '
           '--debug --log-level DEBUG '
           '--execute --to notebook '
           f'{"--allow-errors " if allow_errors else ""}'
           '--ClearMetadataPreprocessor.enabled=True '
           f'--inplace "{nb_file.as_posix()}"'
       )


def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_md(notebook, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
# def jupyter_to_python(notebook, build_dir):
#     """Convert a notebook to a python script"""
#     return f"jupyter nbconvert --log-level WARN --to python ./src/{notebook}.ipynb --output _{notebook}.py --output-dir {build_dir}"
def jupyter_to_python(notebook, build_dir):    # notebook: e.g. "expectations/01_Market_Expectations…_Final"
    nb_path = Path("src") / notebook
    out_name = "_" + nb_path.stem + ".py"
    return (
        f'jupyter nbconvert --log-level WARN '
        f'--to python "{nb_path}.ipynb" '
        f'--output {out_name} '
        f'--output-dir="{build_dir}"'
    )
# def jupyter_clear_output(notebook, output_dir=OUTPUT_DIR):
#     return f"jupyter nbconvert --log-level WARN --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace ./src/{output_dir}/{notebook}.ipynb"
def jupyter_clear_output(notebook_path):
    # notebook_path: e.g. "src/expectations/01_…_Final.ipynb"
    return (
        f'jupyter nbconvert --log-level WARN '
        f'--ClearOutputPreprocessor.enabled=True '
        f'--ClearMetadataPreprocessor.enabled=True '
        f'--inplace "{notebook_path}"'
    )
def jupyter_to_latex(notebook):
    return f"jupyter nbconvert --to latex --output-dir={OUTPUT_DIR} ./src/{notebook}.ipynb"

def task_setup_python_deps():
    """
    Installs runtime-only libraries that some notebooks import
    (e.g. boto3).  It is marked 'uptodate' so it only runs once
    unless you force it or bump the version string.
    """
    require   = ["boto3>=1.34"]
    version   = "1"          # bump to re-run automatically

    return {
        "actions": [f"python -m pip install --quiet {' '.join(require)}"],
        "uptodate": [version],     # cached: only runs if version changes
        "verbosity": 1,
    }

def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################

def task_setup_dirs():
    """Make all of your configured directories up front."""
    def create_all_dirs():
        for d in (DATA_DIR, MANUAL_DATA_DIR,
                  OUTPUT_DIR, OUTPUT_DIR2, OUTPUT_DIR3, OUTPUT_DIR4,
                  PUBLISH_DIR, TEMP_DIR, INPUT_DIR, PROCESSED_DIR):
            os.makedirs(d, exist_ok=True)

    return {
        "actions": [create_all_dirs],
        "verbosity": 0,
    }

# -------------------------  Scraper EC2 launcher  -------------------------
def task_scrapers_run():
    """
    Spin-up the scraper EC2 instances by running
    ``src/scrapers/scrapers_main.py`` and wait for them to terminate.
    All options are passed straight through to the script.
    """
    PY  = PYTHON  # whatever constant you defined for “python”/“ipython”
    exe = Path("src") / "scrapers" / "scrapers_main.py"

    return {
        # The function is entirely executed by the shell command:
        "actions": [(
            "{py} {script}"
            " --s3_prefix={prefix}"
            " --ami_id={ami}"
            " --instance_type={itype}"
            " --key_name={key}"
            " --num_instances={num}"
        ).format(
            py=PY,
            script=exe.as_posix(),
            prefix="%(prefix)s",
            ami="%(ami)s",
            itype="%(itype)s",
            key="%(key)s",
            num="%(num)s",
        )],
        # A tiny flag-file written **after** the script exits is enough to
        # tell doit that the task succeeded once; delete the file to re-run.
        "targets": ["scrapers_done.flag"],

        # ------------------------------------------------------------------
        #  Task-level parameters exposed on the CLI:
        #     doit scrapers_run --help   (to inspect them)
        # ------------------------------------------------------------------
        "params": [
            { "name": "prefix",  "short": "p", "long": "s3_prefix",
              "default": "",                "type": str,
              "help": "S3 key prefix under BUCKET (e.g. projectA/)"},
            { "name": "ami",     "short": "a", "long": "ami_id",
              "default": "ami-00a929b66ed6e0de6", "type": str,
              "help": "AMI to boot each scraper EC2 instance from"},
            { "name": "itype",   "short": "t", "long": "instance_type",
              "default": "t3.micro",        "type": str,
              "help": "EC2 instance type"},
            { "name": "key",     "short": "k", "long": "key_name",
              "default": "vockey",          "type": str,
              "help": "EC2 key-pair name"},
            { "name": "num",     "short": "n", "long": "num_instances",
              "default": 4,    "type": int,
              "help": "How many scraper nodes to launch"},
        ],

        # Always run if the flag-file is missing
        "uptodate": [False],

        # Higher verbosity so you see the EC2 / boto3 progress
        "verbosity": 2,
    }


from src.cluster_setup import start_cluster
def task_start_dask():
    """Start a Dask EC2 cluster, wait for workers, and persist scheduler info."""
    return {
        "actions": [(start_cluster,)],
        "targets": ["dask-scheduler.json"],
        "verbosity": 2,
        # always re‐run if deleted:
        "uptodate": [False],
    }

###############################
## TIPS Treasury 
###############################

section = 'tips_treasury'
def task_tips_treasury_pull_fed_yield_curve():
    """ """
    file_dep = [
        "./src/tips_treasury/pull_fed_yield_curve.py",
    ]
    targets = [
        DATA_DIR / "fed_yield_curve_all.parquet",
        DATA_DIR / "fed_yield_curve.parquet",
    ]

    return {
        "actions": [
            "ipython ./src/tips_treasury/pull_fed_yield_curve.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": [],
    }

def task_tips_treasury_pull_fed_tips_yield_curve():
    """ """
    file_dep = [
        "./src/tips_treasury/pull_fed_tips_yield_curve.py",
    ]
    targets = [
        DATA_DIR / "fed_tips_yield_curve.parquet",
    ]

    return {
        "actions": [
            "ipython ./src/tips_treasury/pull_fed_tips_yield_curve.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": [],
    }

def task_tips_treasury_pull_bloomberg_treasury_inflation_swaps():
    """Run pull_bloomberg_treasury_inflation_swaps only if treasury_inflation_swaps.csv is not present in OUTPUT_DIR."""
    from pathlib import Path  # ensure Path is available
    output_dir = Path(OUTPUT_DIR)

    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        
    if not any(output_dir.iterdir()):
        # Only yield the nested task if the CSV file is not present.
        yield {
            "name": "run",
            "actions": ["ipython ./src/tips_treasury/pull_bloomberg_treasury_inflation_swaps.py"],
            "file_dep": ["./src/tips_treasury/pull_bloomberg_treasury_inflation_swaps.py"],
            "targets": [DATA_DIR / "treasury_inflation_swaps.parquet"],
            "clean": [],
        }
    else:
        print("treasury_inflation_swaps.csv exists in OUTPUT_DIR; skipping task_pull_bloomberg_treasury_inflation_swaps")


def task_tips_treasury_compute_tips_treasury():
    """ """
    file_dep = [
        "./src/tips_treasury/compute_tips_treasury.py",
    ]
    targets = [
        OUTPUT_DIR / "tips_treasury_implied_rf.parquet",
    ]

    return {
        "actions": [
            "ipython ./src/tips_treasury/compute_tips_treasury.py",
        ],
        "targets": targets,
        "uptodate": [True], 
        "file_dep": file_dep,
        "clean": [],
    }

def task_tips_treasury_generate_figures():
    """ """
    file_dep = [
        "./src/tips_treasury/generate_figures.py",
        "./src/tips_treasury/generate_latex_table.py",
    ]
    file_output = [
        "tips_treasury_spreads.png",
        "tips_treasury_summary_stats.csv",
        'tips_treasury_summary_table.tex'
    ]
    targets = [OUTPUT_DIR / file for file in file_output]

    return {
        "actions": [
            "ipython ./src/tips_treasury/generate_figures.py",
            "ipython ./src/tips_treasury/generate_latex_table.py",
        ],
        "targets": targets,
        "uptodate": [True], 
        "file_dep": file_dep,
        "clean": [],
    }


notebook_tasks_tips_t = {
    "arb_replication.ipynb": {
        "file_dep": [
            "./src/tips_treasury/generate_figures.py"
        ],
        "targets": [],
    }
}

def task_tips_treasury_convert_notebooks_to_scripts():
    """Convert tips_treasury notebooks to scripts."""
    section = "tips_treasury"
    tips_notebook_tasks = {
        "arb_replication.ipynb": {
            "file_dep": ["./src/tips_treasury/generate_figures.py"],
            "targets": [],
        }
    }
    build_dir = Path(OUTPUT_DIR)

    for notebook in tips_notebook_tasks:
        notebook_name = notebook[:-6]   # strip ".ipynb"
        src_path = f"src/{section}/{notebook}"
        yield {
            "name": notebook,
            "actions": [
                jupyter_clear_output(src_path),
                jupyter_to_python(f"{section}/{notebook_name}", build_dir),
            ],
            "file_dep": [Path(src_path)],
            "targets": [build_dir / f"_{notebook_name}_tips_treasury.py"],
            "task_dep": ["setup_python_deps"],   
            "clean": True,
            "verbosity": 0,
        }

# fmt: off
def task_tips_treasury_run_notebooks():
    section = "tips_treasury"
    tips_notebooks = {
        "arb_replication.ipynb": {}
    }
    for notebook in tips_notebooks:
        nb_base = notebook[:-6]  # strip ".ipynb"
        src_rel = Path("src") / section / notebook
        yield {
            "name": notebook,
            "actions": [
                # pass in the section/folder, not just the name
                jupyter_execute_notebook(f"{section}/{nb_base}")
            ],
            "file_dep": [src_rel],
            "task_dep": ["setup_python_deps"],   
            "targets": [],
            "uptodate": [True],
            "verbosity": 2,
        }

# fmt: on

def task_tips_treasury_compile_latex_docs():
    """Compile the LaTeX documents to PDFs"""
    file_dep = [
        "./reports/report.tex",
        "./reports/my_article_header.sty",      # style 
        #"./reports/slides_example.tex",
        #`"./reports/my_beamer_header.sty",       # style
        "./reports/my_common_header.sty",       # style
        # "./reports/report_simple_example.tex",
        # "./reports/slides_simple_example.tex",
        "./src/tips_treasury/generate_figures.py",
        "./src/tips_treasury/generate_latex_table.py",
    ]
    targets = [
        "./reports/report.pdf",
        #"./reports/slides_example.pdf",
        # "./reports/report_simple_example.pdf",
        # "./reports/slides_simple_example.pdf",
    ]

    return {
        "actions": [
            # My custom LaTeX templates
            "latexmk -xelatex -halt-on-error -cd ./reports/report.tex",  # Compile
            "latexmk -xelatex -halt-on-error -c -cd ./reports/report.tex",  # Clean
      ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }


###############################
## Equity Spot Futures 
###############################

LOG_FILES = [
    TEMP_DIR / "futures_processing.log",
    TEMP_DIR / "ois_processing.log",
   # TEMP_DIR / "bloomberg_data_extraction.log"
]

# def task_equity_spot_config():
#     """Create empty directories for data and output if they don't exist, and ensure log files are created"""
#     return {
#         "actions": ["ipython ./src/settings.py"],  # This action should ensure directories and files are prepared
#         "targets": [
#             DATA_DIR, OUTPUT_DIR, TEMP_DIR, INPUT_DIR,  PROCESSED_DIR
#         ] ,#+ LOG_FILES,  # Include log files in the targets to manage their existence
#         "file_dep": ["./src/settings.py"],
#         "clean": True,  # This will clean up all directories and log files when 'doit clean' is executed
#     }
section = 'equity_spot'
# def task_equity_spot_pull_bloomberg():
#     """ """
#     file_dep = [
#         "./src/settings.py"
#     ]
#     targets = [
#         INPUT_DIR / "bloomberg_historical_data.parquet"
#     ]

#     return {
#         "actions": [
#             "ipython ./src/equity_spot/pull_bloomberg_data.py",
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": [], 
#     }

def task_equity_spot_process_futures_data():
    """
    Process futures data for indices after pulling the latest data.
    """
    file_dep = [
        "./src/settings.py",
        "./src/equity_spot/pull_bloomberg_data.py",
        "./src/equity_spot/futures_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "all_indices_calendar_spreads.csv",
        PROCESSED_DIR / "INDU_calendar_spread.csv",
        PROCESSED_DIR / "SPX_calendar_spread.csv",
        PROCESSED_DIR / "NDX_calendar_spread.csv",
    ]

    return {
        "actions": [
            "python ./src/equity_spot/futures_data_processing.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  
    }

def task_equity_spot_process_ois_data():
    """
    Process OIS data for 3-month rates after pulling the latest Bloomberg data.
    """
    file_dep = [
        "./src/settings.py",
        "./src/equity_spot/pull_bloomberg_data.py",
        "./src/equity_spot/OIS_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "cleaned_ois_rates.csv"
    ]

    return {
        "actions": [
            "python ./src/equity_spot/OIS_data_processing.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  # Add appropriate clean actions if necessary
    }

def task_equity_spot_spread_calculations():
    """
    Spread calculations from processed data
    """
    file_dep = [
        "./src/settings.py",
        "./src/equity_spot/pull_bloomberg_data.py",
        "./src/equity_spot/OIS_data_processing.py",  
        "./src/equity_spot/futures_data_processing.py"
    ]
    targets = [
        PROCESSED_DIR / "SPX_Forward_Rates.csv",
        PROCESSED_DIR / "NDX_Forward_Rates.csv",
        PROCESSED_DIR / "INDU_Forward_Rates.csv",
        OUTPUT_DIR / "all_indices_spread_to_2020.png",
        OUTPUT_DIR / "all_indices_spread_to_present.png"
    ]

    return {
        "actions": [
            "python ./src/equity_spot/Spread_calculations.py",
        ],
        "file_dep": file_dep,
        "targets": targets,
        "clean": True,  
    }


notebook_tasks = {
    "01_OIS_Data_Processing.ipynb": {
        "file_dep": ["./src/settings.py","./src/equity_spot/pull_bloomberg_data.py", 
                     "./src/equity_spot/OIS_data_processing.py"],
        "targets": [OUTPUT_DIR / 'ois_3m_rolling_statistics.png',
                    OUTPUT_DIR / 'ois_3m_rate_time_series.png',
                    OUTPUT_DIR / "ois_summary_statistics.tex"],
    },
    "02_Futures_Data_Processing.ipynb": {
        "file_dep": ["./src/settings.py",
                     "./src/equity_spot/pull_bloomberg_data.py", 
                     "./src/equity_spot/futures_data_processing.py"],
        "targets": [OUTPUT_DIR / "es1_contract_roll_pattern.png",
                    OUTPUT_DIR / "es1_ttm_distribution.png",
                    OUTPUT_DIR / "futures_prices_by_index.png",],
    },
    "03_Spread_Calculations.ipynb": {
        "file_dep": [
            "./src/settings.py",
            "./src/equity_spot/pull_bloomberg_data.py", 
            "./src/equity_spot/futures_data_processing.py",
            "./src/equity_spot/OIS_data_processing.py",
            "./src/equity_spot/Spread_calculations.py"
        ],
        "targets": [],
    },
}


def task_equity_spot_convert_notebooks_to_scripts():
    """Convert notebooks to script form to detect changes to source code rather
    than to the notebook's metadata.
    """
    section = "equity_spot"
    build_dir = Path(OUTPUT_DIR3)

    for notebook in notebook_tasks.keys():
        nb_base  = notebook[:-6]       # strip ".ipynb"
        src_path = f"src/{section}/{notebook}"

        yield {
            "name": notebook,
            "actions": [
                jupyter_clear_output(src_path),
                jupyter_to_python(f"{section}/{nb_base}", build_dir),
            ],
            "file_dep": [Path(src_path)],
            "targets": [build_dir / f"_{nb_base}.py"],
            "clean":   True,
            "verbosity": 0,
        }


# fmt: off
def task_equity_spot_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """
    section = "equity_spot"
    for notebook in notebook_tasks.keys():
        notebook_name = notebook.split(".")[0]
        yield {
            "name": notebook,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
                jupyter_execute_notebook(f"{section}/{notebook_name}"),
                jupyter_to_html(f"{section}/{notebook_name}"),
                copy_file(
                    Path("./src") / section / f"{notebook_name}.ipynb",
                    OUTPUT_DIR / f"{notebook_name}.ipynb",
                    mkdir=True,
                ),
                copy_file(
                    Path("./src") / section / f"{notebook_name}.ipynb",
                    Path("./_docs/notebooks/") / f"{notebook_name}.ipynb",
                    mkdir=True,
                ),
                jupyter_clear_output(f"src/{section}/{notebook_name}.ipynb"),
                # jupyter_to_python(notebook_name, build_dir),
                """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                OUTPUT_DIR / f"_{notebook_name}.py",
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook_name}.html",
                OUTPUT_DIR / f"{notebook_name}.ipynb",
                *notebook_tasks[notebook]["targets"],
            ],
            "uptodate": [True],
            "clean": True,
        }

notebook_sphinx_pages = [
    "./_docs/_build/html/notebooks/" + notebook.split(".")[0] + ".html"
    for notebook in notebook_tasks.keys()
]
sphinx_targets = [
    "./_docs/_build/html/index.html",
    *notebook_sphinx_pages
]


def copy_docs_src_to_docs():
    """
    Copy all files and subdirectories from the docs_src directory to the _docs directory.
    This function loops through all files in docs_src and copies them individually to _docs,
    preserving the directory structure. It does not delete the contents of _docs beforehand.
    """
    src = Path("docs_src")
    dst = Path("_docs")

    # Ensure the destination directory exists
    dst.mkdir(parents=True, exist_ok=True)

    # Loop through all files and directories in docs_src
    for item in src.rglob("*"):
        relative_path = item.relative_to(src)
        target = dst / relative_path
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            shutil.copy2(item, target)


def copy_docs_build_to_docs():
    """
    Copy all files and subdirectories from _docs/_build/html to docs.
    This function copies each file individually while preserving the directory structure.
    It does not delete any existing contents in docs.
    After copying, it creates an empty .nojekyll file in the docs directory.
    """
    src = Path("_docs/_build/html")
    dst = Path("docs")
    dst.mkdir(parents=True, exist_ok=True)

    # Loop through all files and directories in src
    for item in src.rglob("*"):
        relative_path = item.relative_to(src)
        target = dst / relative_path
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)

    # Touch an empty .nojekyll file in the docs directory.
    (dst / ".nojekyll").touch()


# def task_equity_spot_compile_sphinx_docs():
#     """Compile Sphinx Docs"""
#     notebook_scripts = [
#         OUTPUT_DIR / ("_" + notebook.split(".")[0] + ".py")
#         for notebook in notebook_tasks.keys()
#     ]
#     file_dep = [
#         "./docs_src/conf.py",
#         "./docs_src/index.md",
#         *notebook_scripts,
#     ]

#     return {
#         "actions": [
#             copy_docs_src_to_docs,
#             "sphinx-build -M html ./_docs/ ./_docs/_build",
#             copy_docs_build_to_docs,
#         ],
#         "targets": sphinx_targets,
#         "file_dep": file_dep,
#         "task_dep": ["run_notebooks"],
#         "clean": True,
#     }

# def task_equity_spot_compile_latex_docs():
#     """Compile the LaTeX documents to PDFs"""
#     file_dep = [
#         "./reports/equity_spot_report.tex"
#     ]
#     targets = [
#         "./reports/equity_spot_report.pdf"
#     ]

#     return {
#         "actions": [
#             "latexmk -xelatex -halt-on-error -cd ./reports/equity_spot_report.tex",  # Compile
#             "latexmk -xelatex -halt-on-error -c -cd ./reports/equity_spot_report.tex"  # Clean
#         ],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }

###############################
## CIP
###############################
 
section = 'CIP'
def write_bloomberg(mode):
    """
    mode: 'Y' or 'N' (case‐insensitive).  Writes BLOOMBERG = True/False into src/settings.py.
    """
    new_val = "True" if mode.upper().startswith("Y") else "False"
    settings_path = Path("src/settings.py")

    lines = settings_path.read_text().splitlines(keepends=True)
    for i, line in enumerate(lines):
        if line.strip().startswith("BLOOMBERG ="):
            lines[i] = f"BLOOMBERG = {new_val}\n"
            break
    else:
        # if not found, append it
        lines.append(f"\nBLOOMBERG = {new_val}\n")

    settings_path.write_text("".join(lines))
    print(f"✔️  Set BLOOMBERG = {new_val} in src/settings.py")


def task_CIP_BLOOMBERG():
    """
    Update the BLOOMBERG flag in src/settings.py.
    Usage:
      doit CIP_BLOOMBERG --mode=Y
      doit CIP_BLOOMBERG -m Y
    """
    return {
        "actions": [(write_bloomberg, ["%(mode)s"])],
        "params": [
            {
                "name": "mode",
                "short": "m",
                "long": "mode",
                "type": str,
                "default": "N",
                "help": "Run from Bloomberg terminal? [Y/N]",
            }
        ],
        "verbosity": 2,
    }


def task_download_cip_data():
    """
    Download CIP_2025.xlsx from GitHub and save it to the manual data folder.
    """
    target_file = MANUAL_DATA_DIR / "CIP_2025.xlsx"

    def download():
        import requests
        url = "https://raw.githubusercontent.com/Kunj121/CIP_DATA/main/CIP_2025%20(1).xlsx"
        response = requests.get(url)
        response.raise_for_status()
        os.makedirs(MANUAL_DATA_DIR, exist_ok=True)
        with open(target_file, "wb") as f:
            f.write(response.content)
        print(f"File saved to {target_file.resolve()}")

    return {
        "actions": [download],
        "targets": [str(target_file)],
        "uptodate": [False],
        "clean": True,
    }



def task_CIP_clean_data():
    """Run the CIP data cleaning script."""
    tidy_data_file = DATA_DIR / "tidy_data.csv"
    return {
        "actions": ["ipython ./src/CIP/clean_data.py"],
        "file_dep": [str(MANUAL_DATA_DIR / "CIP_2025.xlsx")],
        "targets": [str(tidy_data_file)],
        "clean": True,
    }

def task_CIP_run_notebooks():
    """Execute the main CIP notebook and export to HTML/LaTeX."""
    section = "CIP"
    nb = "main_cip"
    src_nb = Path("src") / section / f"{nb}.ipynb"
    html_dir = OUTPUT_DIR / "html_files"
    html_dir.mkdir(parents=True, exist_ok=True)
    html_file = html_dir / f"{nb}.html"
    tex_file  = OUTPUT_DIR / f"{nb}.tex"
    excel    = MANUAL_DATA_DIR / "CIP_2025.xlsx"

    return {
        "actions": [
            # note: pass section/nb into our helper so it looks under src/CIP/main_cip.ipynb
            jupyter_execute_notebook(f"{section}/{nb}"),
            # write the HTML to html_dir
            jupyter_to_html(f"{section}/{nb}", output_dir=html_dir),
            # default jupyter_to_latex writes to OUTPUT_DIR
            jupyter_to_latex(f"{section}/{nb}"),
        ],
        "file_dep": [
            str(src_nb),
            str(excel),
        ],
        "targets": [
            str(html_file),
            str(tex_file),
        ],
        "task_dep": ["download_cip_data"],
        "clean": True,
    }



def task_CIP_rename_plots():
    """Run CIP analysis and rename output plots correctly."""
    script = Path("src") / "CIP" / "cip_analysis.py"
    output_dir = OUTPUT_DIR / "main_cip_files"
    excel = MANUAL_DATA_DIR / "CIP_2025.xlsx"

    def rename_output_files():
        output_dir.mkdir(parents=True, exist_ok=True)

        file_patterns = {
            r"main_cip_\d+_0\.png": "cip_spread_plot_replication.png",
            r"main_cip_\d+_1\.png": "cip_spread_plot_2025.png",
        }
        matched = set()

        for old in output_dir.glob("main_cip_*.png"):
            for pattern, new_name in file_patterns.items():
                if re.match(pattern, old.name):
                    new = output_dir / new_name
                    # avoid accidental overwrite
                    if new.exists() and new_name == "cip_spread_plot_2025.png":
                        new = output_dir / "cip_spread_2025.png"
                    old.rename(new)
                    print(f"Renamed {old.name} → {new.name}")
                    matched.add(old.name)

        # warn about any leftovers
        for leftover in output_dir.glob("main_cip_*.png"):
            if leftover.name not in matched:
                print(f"Warning: no rename rule for {leftover.name}")

    return {
        "actions": [
            # run your analysis script
            f"ipython {script.as_posix()}",
            # then rename the generated plots
            rename_output_files,
        ],
        "file_dep": [
            str(script),
            str(Path("src") / "CIP" / "pull_bloomberg_cip_data.py"),
            str(excel),
        ],
        "task_dep": [
            "download_cip_data",    # your data‐download task
            "CIP_run_notebooks",    # ensure the notebook has already been executed
        ],
        "targets": [
            str(output_dir / "cip_spread_plot_replication.png"),
            str(output_dir / "cip_spread_plot_2025.png"),
        ],
        "clean": True,
        "verbosity": 2,
    }


def task_CIP_summary_stats():
    """Generate CIP summary statistics and save them as HTML files."""
    from pathlib import Path
    import sys

    # Where our CIP code lives:
    cip_src = Path("src") / "CIP"
    # Ensure Python can import from src/CIP
    sys.path.insert(0, str(cip_src))

    # Inputs:
    deps = [
        str(cip_src / "pull_bloomberg_cip_data.py"),
        str(cip_src / "cip_analysis.py"),
        str(cip_src / "directory_functions.py"),
        str(MANUAL_DATA_DIR / "CIP_2025.xlsx"),
    ]
    # Outputs:
    out1 = OUTPUT_DIR / "cip_summary_overall.html"
    out2 = OUTPUT_DIR / "cip_correlation_matrix.html"
    out3 = OUTPUT_DIR / "cip_annual_statistics.html"
    outputs = [str(out1), str(out2), str(out3)]

    def generate_summary():
        # Make sure OUTPUT_DIR exists
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # import our routines
        from pull_bloomberg_cip_data import compute_cip
        from cip_analysis import compute_cip_statistics
        from directory_functions import save_cip_statistics_as_html

        # 1) get the raw CIP series
        cip_data = compute_cip()

        # 2) compute the stats
        stats_dict = compute_cip_statistics(cip_data)
        if "overall_statistics" not in stats_dict:
            raise KeyError("`overall_statistics` missing from stats_dict")

        # 3) save them into HTML
        #    save_cip_statistics_as_html() knows to write into OUTPUT_DIR/html_files
        save_cip_statistics_as_html(stats_dict)

    return {
        "actions": [generate_summary],
        "file_dep":    deps,
        "targets":     outputs,
        "task_dep":    ["download_cip_data", "CIP_run_notebooks"],
        "clean":       True,
        "verbosity":   2,
    }


import shutil


def _ensure_publish_dir():
    PUBLISH_DIR.mkdir(parents=True, exist_ok=True)

def copy_notebook():
    """Copy main_cip.ipynb from src/CIP to PUBLISH_DIR and rename it paper.ipynb."""
    src_nb = Path("src") / "CIP" / "main_cip.ipynb"
    dest_nb = PUBLISH_DIR / "paper.ipynb"
    shutil.copy2(src_nb, dest_nb)
    print(f"Copied {src_nb} → {dest_nb}")

def task_CIP_generate_paper():
    """Generate a PDF paper from the copied notebook."""
    paper_nb = PUBLISH_DIR / "paper.ipynb"
    paper_tex = PUBLISH_DIR / "paper.tex"
    paper_pdf = PUBLISH_DIR / "paper.pdf"

    return {
        "actions": [
            _ensure_publish_dir,
            copy_notebook,
            # Execute the notebook in place (allowing errors so that output is preserved)
            (
                'jupyter nbconvert '
                f'--execute --to notebook --inplace '
                f'--ExecutePreprocessor.allow_errors=True '
                f'"{paper_nb.as_posix()}"'
            ),
            # Convert to LaTeX
            (
                'jupyter nbconvert '
                f'--to latex '
                f'--output-dir="{PUBLISH_DIR.as_posix()}" '
                f'"{paper_nb.as_posix()}"'
            ),
            # Build PDF (twice to get TOC, refs, etc.)
            f'pdflatex -output-directory="{PUBLISH_DIR.as_posix()}" "{paper_tex.as_posix()}"',
            f'pdflatex -output-directory="{PUBLISH_DIR.as_posix()}" "{paper_tex.as_posix()}"',
        ],
        "file_dep": [
            "src/CIP/main_cip.ipynb",
            # any images/scripts/etc you embed in the notebook?
        ],
        "targets": [str(paper_pdf)],
        "clean": True,
        "verbosity": 2,
    }

def task_CIP_clean_reports():
    """Remove auxiliary files left over from pdflatex."""
    aux_files = ["paper.aux", "paper.log", "paper.out", "paper.tex"]
    aux_paths = [PUBLISH_DIR / fn for fn in aux_files]

    def remove_aux():
        for f in aux_paths:
            if f.exists():
                f.unlink()
                print(f"Removed {f.name}")
    
    return {
        "actions": [remove_aux],
        "file_dep": [],     # no need; this is purely a cleanup
        "clean": True,      # allow `doit clean` to run it
        "verbosity": 1,
    }

###############################
## Treasury Spot Futures 
###############################


section = "treasury_spot"

def task_treasury_spot_clean_raw():
    """Run clean_raw.ipynb to generate treasury_df, ois_df, last_day_df."""
    nb_path = "treasury_spot/clean_raw"
    src_nb   = Path("src") / f"{nb_path}.ipynb"

    return {
        "actions": [
            # force forward-slash path into nbconvert
            jupyter_execute_notebook(nb_path),
        ],
        "file_dep": [src_nb],
        "targets": [
            DATA_DIR / "treasury_df.csv",
            DATA_DIR / "ois_df.csv",
            DATA_DIR / "last_day_df.csv",
        ],
        "clean": True,
    }

def task_treasury_spot_generate_reference():
    """Run generate_reference.py to create reference.csv."""
    return {
        "actions": ["python src/treasury_spot/generate_reference.py"],
        "file_dep": ["src/treasury_spot/generate_reference.py"],
        "targets": [DATA_DIR / "reference.csv"],
        "clean": True,
    }

def task_treasury_spot_calc_treasury_data():
    """Process Treasury SF data and produce treasury_sf_output.csv."""
    return {
        "actions": ["python src/treasury_spot/calc_treasury_data.py"],
        "file_dep": [
            "src/treasury_spot/calc_treasury_data.py",
            DATA_DIR / "treasury_df.csv",
            DATA_DIR / "ois_df.csv",
            DATA_DIR / "last_day_df.csv",
        ],
        "targets": [DATA_DIR / "treasury_sf_output.csv"],
        "clean": True,
    }

def task_treasury_spot_process_treasury_data_notebook():
    """Run process_treasury_data.ipynb to further process SF output."""
    section = "treasury_spot"
    nb = f"{section}/process_treasury_data"
    src_nb = Path("src") /f"{nb}.ipynb"
    return {
        "actions": [jupyter_execute_notebook(nb)],
        "file_dep": [
            src_nb,
            DATA_DIR / "treasury_sf_output.csv",
        ],
        # if this writes out any specific files you care about, list them here
        "clean": True,
    }

def task_treasury_spot_plot_exploration():
    """Run 01_explore_basis_trade_data_new.ipynb for visualization."""
    section = "treasury_spot"
    nb = f"{section}/01_explore_basis_trade_data_new"
    src_nb = Path("src") / f"{nb}.ipynb"
    return {
        "actions": [jupyter_execute_notebook(nb)],
        "file_dep": [
            src_nb,
            DATA_DIR / "treasury_sf_output.csv",
        ],
        # again, list any plots/html you care about under `targets` if you want caching
        "clean": True,
    }

def task_treasury_spot_test_calc_treasury_data():
    """Unit tests for calc_treasury_data.py."""
    return {
        "actions": ["pytest src/treasury_spot/test_calc_treasury_data.py"],
        "file_dep": [
            "src/treasury_spot/calc_treasury_data.py",
            DATA_DIR / "treasury_sf_output.csv",
            DATA_DIR / "reference.csv",
        ],
        "task_dep": ["treasury_spot_generate_reference"],
        "clean": True,
    }

def task_treasury_spot_test_load_bases_data():
    """Unit tests for load_bases_data.py."""
    return {
        "actions": ["pytest src/treasury_spot/test_load_bases_data.py"],
        "file_dep": [
            "src/treasury_spot/load_bases_data.py",
            DATA_DIR / "reference.csv",
        ],
        "clean": True,
    }

# def task_treasury_spot_latex_to_document():
#     """Convert a LaTeX report into a PDF."""
#     tex = Path("reports") / "Final_Report.tex"
#     pdf = Path("reports") / "Final_Report.pdf"
#     return {
#         "actions": [f"python src/treasury_spot/latex_to_document.py {tex}"],
#         "file_dep": [
#             "src/treasury_spot/latex_to_document.py",
#             str(tex),
#         ],
#         "targets": [str(pdf)],
#         "clean": True,
#     }

###############################
## Market Expectations 
###############################

# def task_expectations_config():
#     """Create empty directories for data, output, plots, and tables if they don't exist"""
#     return {
#         "actions": ["ipython ./src/settings.py"],
#         "targets": [DATA_DIR, OUTPUT_DIR, PLOTS_DIR, TABLES_DIR],
#         "file_dep": ["./src/settings.py"],
#         "clean": [],  # Don't clean these files by default.
#     }

# This is needed for F-F BE/ME portfolios

section = "expectations"
PYTHON = sys.executable
def task_expectations_pull_ken_french_data():
    """Pull data from Ken French's website."""
    script = Path("src") / section / "pull_ken_french_data.py"
    return {
        "actions": [
            "ipython src/settings.py",
            f"ipython {script}"
        ],
        "file_dep": [
            "src/settings.py",
            str(script),
        ],
        "targets": [
            DATA_DIR / "6_Portfolios_2x3.xlsx",
            DATA_DIR / "25_Portfolios_5x5.xlsx",
            DATA_DIR / "100_Portfolios_10x10.xlsx",
        ],
        "clean": [],   # keep these around
        "verbosity": 2,
    }

def task_expectations_pull_CRSP_index():
    """Pull CRSP value-weighted index."""
    script = Path("src") / section / "pull_CRSP_index.py"
    return {
        "actions": [f"{PYTHON} {script.as_posix()}"],
        "file_dep": [str(script)],
        "targets": [DATA_DIR / "crsp_value_weighted_index.csv"],
        "clean": [],
    }

def task_expectations_convert_notebooks_to_scripts():
    """Convert expectation notebooks to .py scripts for diffing."""
    build_dir = Path(OUTPUT_DIR2)
    for nb in ["01_Market_Expectations_In_The_Cross-Section_Of_Present_Values_Final.ipynb",
               "run_regressions.ipynb"]:
        name = Path(nb).stem
        src_nb = Path("src") / section / nb
        yield {
            "name": name,
            "actions": [
                jupyter_clear_output(str(src_nb)),
                jupyter_to_python(f"{section}/{name}", build_dir),
            ],
            "file_dep": [src_nb],
            "targets": [build_dir / f"_{name}.py"],
            "clean": True,
            "verbosity": 0,
        }

def task_expectations_run_notebooks():
    """Execute expectation notebooks and export to HTML, skipping cell errors."""
    from pathlib import Path
    build_dir = Path(OUTPUT_DIR2)
    build_dir.mkdir(parents=True, exist_ok=True)

    for nb in [
        "01_Market_Expectations_In_The_Cross-Section_Of_Present_Values_Final.ipynb",
        "run_regressions.ipynb",
    ]:
        name   = Path(nb).stem
        src_nb = Path("src") / section / nb
        html_tgt = build_dir / f"{name}.html"
        ipynb_tgt = build_dir / nb

        yield {
            "name": name,
            "actions": [
                # execute in-place, but keep going on errors
                f"{PYTHON} -m nbconvert "
                f"--execute "
                f"--to notebook "
                f"--allow-errors "
                f"--ClearMetadataPreprocessor.enabled=True "
                f"--inplace \"{src_nb.as_posix()}\"",
                # export to HTML
                f"{PYTHON} -m nbconvert "
                f"--to html "
                f"--output-dir=\"{build_dir.as_posix()}\" "
                f"\"{src_nb.as_posix()}\"",
            ],
            "file_dep": [
                str(src_nb),
                str(build_dir / f"_{name}.py"),
            ],
            "targets": [
                str(html_tgt),
                str(ipynb_tgt),
            ],
            "clean": True,
        }


def task_expectations_compile_latex_docs():
    """Compile the main LaTeX report for expectations."""
    tex = Path("reports") / "project.tex"
    pdf = Path("reports") / "project.pdf"
    return {
        "actions": [
            f"latexmk -xelatex -halt-on-error -cd {tex}",
            f"latexmk -xelatex -halt-on-error -c -cd {tex}",
        ],
        "file_dep": [str(tex)],
        "targets": [str(pdf)],
        "clean": True,
    }