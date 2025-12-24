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

out_path_order_miss=paths.reports/"missingness_orders.csv"

out_path_order_par=paths.processed/"orders.parquet"

out_path_users_par=paths.processed/"users.parquet"
out_path_order_clean_par=paths.processed/"orders_clean.parquet"




Data_f=read_orders_csv(data_path)
Data_f2=read_users_csv(data_path2)






require_columns(Data_f,['order_id','user_id','amount','quantity','created_at','status'])
require_columns(Data_f2,['user_id','country','signup_date'
])


assert_non_empty(Data_f, "orders_clean")
assert_non_empty(Data_f2, "users")


enforce_schema(Data_f)


write_parquet(Data_f2,out_path_users_par)

write_parquet(Data_f,out_path_order_par)

missingness_report_df=missingness_report(Data_f) #write out as csv in this path out_path_order_miss
missingness_report_df.to_csv(out_path_order_miss, index=True)


mapping = {
 "paid": "paid"
,
 "refund": "refund"
,
"refunded": "refund"
,
 }





Data_f["status_clean"] = apply_mapping(
    normalize_text(Data_f["status"]),
    mapping
)

order_columns= Data_f[["quantity","amount"]]
Data_f_update =  add_missing_flags(Data_f ,["amount", "quantity"]
)




write_parquet(Data_f_update ,out_path_order_clean_par)
