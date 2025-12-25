
# doData ETL + EDA Project

This project is part of a **milestone in the AI Professionals Bootcamp**. It focuses on **exploratory data analysis (EDA)**,
**ETL pipelines**, data manipulation with **pandas**, visualization, and developing analytical skills including storytelling from data insights.

---

## Project Overview

The pipeline performs the following steps:

1. **Extract**: Load raw CSV files (`orders.csv` and `users.csv`)  
2. **Transform**: Clean, normalize, and enrich the data  
3. **Load**: Write processed tables and analytics outputs to Parquet  
4. **EDA**: Explore revenue trends, order distribution, order status, refund rates, and generate visualizations  


---

## Prerequisites

- Python 3.10+  
- Git installed  
- (Recommended) Virtual environment support
  
> **Note:** All commands assume you are in the project root directory (`week2-data-work`) and the `.venv` is active.

---
## Setup
week2-data-work> python -m venv .venv

# activate venv

-#Windows (PowerShell)#:
python -m venv venv
.\venv\Scripts\Activate.ps1

-#Mac / Linux:#:
python3 -m venv venv
source venv/bin/activate

# Install the requirements:

pip install -r requirements.txt



## Run ETL
1.Set python path:
week2-data-work >: $env:PYTHONPATH="src"
2.Run ETL:
week2-data-work >:python scripts/run_etl.py



## Outputs
 data/processed/orders_clean.parquet
 data/processed/users.parquet
 data/processed/analytics_table.parquet
 data/processed/_run_meta.json
 reports/figures/*.png

## EDA
Open notebooks/eda.ipynb and run all cells.
> **Note:** Before running the cells, make sure to select the kernel corresponding to your .venv virtual environment. This ensures all dependencies are available.
