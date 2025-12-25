from pathlib import Path
import pandas as pd
import sys
import re


ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet ,read_parquet
from bootcamp_data.transforms import enforce_schema , missingness_report , add_missing_flags,dedupe_keep_latest,apply_mapping,normalize_text,parse_datetime , add_time_parts, winsorize ,add_outlier_flag

from bootcamp_data.quality import require_columns , assert_non_empty , assert_unique_key

from bootcamp_data.joins import safe_left_join
paths=make_paths(ROOT)
data_path=paths.processed/"orders_clean.parquet"
data_path2=paths.processed/"users.parquet"

out_path=paths.processed/"analytics_table.parquet"






Data_orders=read_parquet(data_path)
Data_user=read_parquet(data_path2)

require_columns(Data_orders,['order_id','user_id','amount','quantity','created_at','status'])
require_columns(Data_user,['user_id','country','signup_date'
])


assert_non_empty(Data_orders, "orders_clean")
assert_non_empty(Data_user, "users")
assert_unique_key(Data_orders ,'order_id')
assert_unique_key(Data_user ,'user_id')

Data_orders_updated=parse_datetime(Data_orders,'created_at')
Data_orders_updated=add_time_parts(Data_orders_updated ,'created_at')




orders_users_join = safe_left_join(
    Data_orders_updated,
    Data_user,
    "user_id",
    validate="many_to_one"
)

orders_users_join['amount'] = pd.to_numeric(
    orders_users_join['amount'], errors='coerce'
)

orders_users_join['amount']= winsorize(orders_users_join['amount'])

write_parquet(orders_users_join ,out_path)

print(f"Order before{len(Data_orders_updated)}")
print(f"users before{len(Data_user)}")
print(f"users before{len(orders_users_join)}")


#7task
country_stats = (
    orders_users_join
    .groupby("country", as_index=False)
    .agg(
        total_revenue=pd.NamedAgg(column="amount", aggfunc="sum"),
        order_count=pd.NamedAgg(column="order_id", aggfunc="count")
    )
)

# small table to terminal
print(country_stats.head())


reports_path = paths.reports /"revenue_by_country.csv"
country_stats.to_csv(reports_path, index=False)
