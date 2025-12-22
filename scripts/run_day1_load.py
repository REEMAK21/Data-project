

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


paths=make_paths(ROOT)
data_path=paths.raw/"orders.csv"
out_path=paths.processed/'orders.parquet'
#------day1
Data_f=read_orders_csv(data_path)
# print(Data_f.count())
# print(Data_f.dtypes)

# Data_f=enforce_schema(Data_f)
write_parquet(Data_f,out_path)

    
#------day3

def assert_unique_key(df,key,allow_na=False):
    df_new=df[key]
    if( df_new.duplicated().sum()>0)or(df_new.isnull().sum()>0):
                        return "not unique key"
    return "is unique key"


def missingness_report(df):
     n=len(df)
     return(
        df.isna().sum()
       .rename("missing_n")
        .to_frame()
        .assign(p_missing=lambda t :t["missing_n"]/n)
        .sort_values("p_missing",ascending=False)
        )


# print(assert_unique_key(Data_f,"order_id"))


print(missingness_report(Data_f))




