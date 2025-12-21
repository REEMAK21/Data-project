

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


paths=make_paths(ROOT)
data_path=paths.raw/"orders.csv"
out_path=paths.processed/'orders.parquet'

Data_f=read_orders_csv(data_path)
print(Data_f.count())
print(Data_f.dtypes)

Data_f=enforce_schema(Data_f)
write_parquet(Data_f,out_path)

    