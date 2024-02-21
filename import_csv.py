import pandas as pd
import csv


def import_csv(input_csv, data_types={}):
    """
    fonction d'importation de csv.
    :param input_csv: fichier à importer
    :param data_types: un dictionnaire de colonnes avec le datatype
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
                dtype=data_types
            )
        )
    return df

