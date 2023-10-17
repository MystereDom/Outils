import pandas as pd


def clean_full_table(path, dict_rename):
    """
    fonction qui...
    :param path: chemin d'accès du csv d'input
    :param dict_rename: dictionnaire de concordance des champs à renommer
    :return: NA. Exporte un csv clean et renommé
    """
    df = pd.read_csv(
        filepath_or_buffer=path,
        usecols=list(dict_rename.keys()),
        sep=",",
        dtype={
            "NOM_GÉO": "string"
        }
        )
    #TODO : Extraire le QUébec seulement
    df = df.rename(columns=dict_rename)
    df.to_csv(
        f'C:/Projets_Python/Outils_Demo/inputs/full_profile_var_pertinentes_2021.csv',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
