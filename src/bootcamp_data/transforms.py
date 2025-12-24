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



def parse_datetime(df, col:str,utc=True)-> pd.DataFrame:
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

       



def add_time_parts(df, ts_col)-> pd.DataFrame:
     times= df[ts_col]
     return df.assign( dates=times.dt.date ,month=times.dt.to_period("M").astype("string"),
           year=times.dt.year, day_week=times.dt.day_name(), hour=times.dt.hour,)


def iqr_bounds(sre:pd.Series, k=1.5):
    c=sre.dropna()
    q1=c.quantile(0.25)
    q3=c.quantile(0.75)
    iqr=q3-q1
    lower = q1 - (k * iqr)
    upper = q3 + (k * iqr)

    return lower ,upper


def winsorize(s, low=0.01, hi=0.99):
    clean_s=s.dropna()
    a,b =clean_s.quantile(low), clean_s.quantile(hi)
    return s.clip(lower=low , upper=b)

def add_outlier_flag(df,col,k=1.5):
    n_col=df[col]
    lower,higer=iqr_bounds(n_col, k=k)
    return df.assign(**{f"{col}_is_outlier": (n_col<lower) | (n_col>higer)})




      

      

