from pathlib import Path
import pandas as pd
import sys
import re
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema , missingness_report , add_missing_flags,dedupe_keep_latest,apply_mapping,normalize_text

from bootcamp_data.quality import require_columns , assert_non_empty

paths=make_paths(ROOT)
data_path=paths.raw/"orders.csv"
data_path2=paths.raw/"users.csv"

out_path=paths.reports/"orders_clean.parquet"





Data_f=read_orders_csv(data_path)
Data_f2=read_users_csv(data_path2)





order_columns= Data_f[["quantity","amount"]]
Data_f_update =  add_missing_flags(Data_f ,["amount", "quantity"]
)


mapping = {
 "paid": "paid"
,
 "refund": "refund"
,
"refunded": "refund"
,
 }





Data_f_update["status_clean"] = apply_mapping(
    normalize_text(Data_f_update["status"]),
    mapping
)



write_parquet(Data_f_update ,out_path)
