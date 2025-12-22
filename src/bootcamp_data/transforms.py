import pandas as pd
import re

def enforce_schema(df: pd.DataFrame)-> pd.DataFrame:
    return df.assign(
    order_id=df["order_id"].astype("string"),
    user_id=df["user_id"].astype("string"),
    amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
    quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )



def missingness_report(df)-> pd.DataFrame:
     n=len(df)
     return(
        df.isna().sum()
       .rename("missing_n")
        .to_frame()
        .assign(p_missing=lambda t :t["missing_n"]/n)
        .sort_values("p_missing",ascending=False)
        )





        
def add_missing_flags(df: pd.DataFrame, cols: list[str])->pd.DataFrame:
            out = df.copy()
            for c in cols:
                out[f"{c} _isna"] = out[c].isna()
                return out



def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str)->pd.DataFrame:

        return (
        df.sort_values(ts_col)
        .drop_duplicates(subset=key_cols, keep="last")
        .reset_index(drop=True)
        )


_ws=re.compile(r"\s+")
def normalize_text(s):
        return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True) )


mapping = {
 "paid": "paid"
,
 "refund": "refund"
,
"refunded": "refund"
,
 }




def apply_mapping(s: pd.Series, mapping: dict[str, str])->pd.Series:
        return s.map(lambda x: mapping.get(x, x))



add_missing_flags,dedupe_keep_latest,apply_mapping,normalize_text
