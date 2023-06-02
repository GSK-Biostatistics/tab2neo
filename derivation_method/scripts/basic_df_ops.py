import pandas as pd


def multiply_cols(df: pd.DataFrame, values: list, out_col:str)->pd.DataFrame:
    df[out_col] = df.loc[:, values].prod(axis=1)
    return df
