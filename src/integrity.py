from __future__ import annotations
import pandas as pd

def check_duplicates(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    d = df.duplicated(subset=keys, keep=False)
    return df.loc[d, keys].drop_duplicates()

def check_ranges(df: pd.DataFrame, col_ranges: dict[str, tuple[float, float]]) -> pd.DataFrame:
    issues = []
    for col, (lo, hi) in col_ranges.items():
        if col in df:
            bad = df[(df[col] < lo) | (df[col] > hi)]
            if not bad.empty:
                issues.append(bad.assign(_col=col, _lo=lo, _hi=hi))
    return pd.concat(issues, ignore_index=True) if issues else pd.DataFrame()