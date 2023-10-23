import pandas as pd


def clean_sr(input_path, output_path, dict_champs):
    """
    fonction qui extrait les SR du Québec d'un .csv
    :param input_path: chemin d'accès du csv d'input
    :param output_path: chemin d'accès du csv d'output
    :param dict_champs: dictionnaire de concordance des champs à renommer
    :return: NA. Exporte un csv clean et renommé
    """
    df = pd.read_csv(
        filepath_or_buffer=input_path,
        sep=",",
        dtype={
            "NOM_GÉO": "string"
        }
        )

    df = df[df["NOM_GÉO"].str.startswith(('408', '421', '442', '433', '462', '505'))]

    # Enlever les SR d'Ottawa
    # convertir le code_geo en nombre
    df['temp_ottawa'] = (
        df['NOM_GÉO']
        .astype('float')
    )
    df = (
        df[~((df['temp_ottawa'] >= 5051000) & (df['temp_ottawa'] < 5060000))]
        .drop('temp_ottawa', axis=1)
        .rename(
            mapper=dict_champs,
            axis=1
        )
    )
    df = df[list(dict_champs.values())]
    df.to_csv(
        path_or_buf=output_path,
        sep=";",
        encoding='utf-8-sig',
        index=False
    )


def clean_sdr(input_path, output_path, dict_champs):
    """
    importe, nettoie puis exporte le fichier du profil du recsenement par SDR du Québec
    :param input_path:
    :param output_path:
    :param dict_champs:
    :return:
    """
    df = pd.read_csv(
        filepath_or_buffer=input_path,
        sep=",",
        dtype={
            "CODE_GÉO_ALT": "string"
        }
    )
    df = df.rename(
        mapper=dict_champs,
        axis=1
    )
    df = df[list(dict_champs.values())]
    df.to_csv(
        path_or_buf=output_path,
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
