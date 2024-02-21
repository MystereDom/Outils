#

import pandas as pd
from extract_by_value import extract_by_value as ebv


def export_extract_long(path, vars_to_extract, colkeep, var_dtypes):
    full_df = (
        pd
        .read_csv(
            f'{path}',
            sep=";",
            dtype={
                "GEO_CODE": "string",
                "MODALITE_NOM": "category",
                "MODALITE_ID": "int16"
            }
        )
        [colkeep]
    )
    df_long = ebv(
        input_df=full_df,
        colname="MODALITE_ID",
        varvalue=[ids for sublist in [list(id_values) for id_values in vars_to_extract.values()] for ids in sublist]
        )
    df_long = df_long.replace({"...": 0, "x": 0, "F": 0, "..": 0})
    df_long[["TOTAL", "H", "F"]] = df_long[["TOTAL", "H", "F"]].fillna(0)
    df_long = df_long.astype(var_dtypes)
    return df_long


def export_extract_long_sdr(path, vars_to_extract, colkeep, var_dtypes):
    full_df = (
        pd
        .read_csv(
            f'{path}',
            sep=";",
            dtype={
                "GEO_CODE": "string",
                "MODALITE_NOM": "category",
                "MODALITE_ID": "int16"
            }
        )
        [colkeep]
    )

    df_long = ebv(
        input_df=full_df,
        colname="MODALITE_ID",
        varvalue=[ids for sublist in [list(id_values) for id_values in vars_to_extract.values()] for ids in sublist]
    )
    df_long[["TOTAL", "H", "F"]] = df_long[["TOTAL", "H", "F"]].fillna(0)
    df_long = df_long.astype(var_dtypes)
    return df_long
