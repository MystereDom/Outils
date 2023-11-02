# Fonction pour joindre des tables de données
import pandas as pd
import csv
# import simpledbf


def import_csv(input_csv):
    """
    fonction d'importation de csv.
    Si un champs "SR" existe, il est importé en format string
    :param input_csv: fichier à importer
    :return: un df
    """
    with open(input_csv, newline="") as csvfile:
        # noinspection PyTypeChecker
        dialect = csv.Sniffer().sniff(csvfile.readline(), delimiters=[',', ';'])
        # noinspection PyTypeChecker
        df = (
            pd
            .read_csv(
                filepath_or_buffer=input_csv,
                sep=dialect.delimiter,

                dtype={"SR": "string"}
            )
        )
    return df


def join_tables(input1, input2, join_field, howmethod='left'):
    """

    :param input1: df, csv ou dbf
    :param input2:
    :param join_field:
    :param howmethod:
    :return:
    """

    df1 = input_to_df(input1)
    # valider que la colonne de joint existe.
    assert join_field in (list(df1.columns)), "Le champs de jointure n'existe pas dans la table 1"
    df2 = input_to_df(input2)
    assert join_field in (list(df2.columns)), "Le champs de jointure n'existe pas dans la table 2"
    df = (
        df1
        .merge(
            right=df2,
            on=join_field,
            how=howmethod
        )
    )
    return df


def input_to_df(input_table):
    if isinstance(input_table, pd.DataFrame):
        df = input_table
        return df
    elif isinstance(input_table, str):
        if input_table[-3:] == 'csv':
            df = import_csv(input_table)
            return df
        elif input_table[-3:] == 'dbf':
            # df = importdbf...
            # return df
            pass
    else:
        df = pd.DataFrame()
        print("data type not Pandas, csv or dbf")


pass
