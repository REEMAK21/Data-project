import pandas as pd
import 
def assert_unique_key(df,key,allow_na=False):
    df_new=df[key]
    print(df_new.unique())
    print(df_new.isna.sum())



df=pd.read_csv()


print(assert_unique_key(df,"order_id"))
