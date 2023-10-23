# Outil pour extraire les données pertinentes du fichier de profil du recensement.

import pandas as pd
import numpy as np
from long_to_wide import long_to_wide as ltw
from extract_by_value import extract_by_value as ebv
from profil_traitement_csv import clean_sr
from profil_traitement_csv import clean_sdr

# ----------------------------------------------------------------------------------------------------------------------
# ### PARAMS #################################################################################
# dictionnaire des chemins d'accès
paths = {
    "path_input_folder": f'C:/Projets_Python/Outils_Demo/inputs/',
    "path_output_folder": f'C:/Projets_Python/Outils_Demo/outputs/',
    "profil_rmr_ar_sr": f'98-401-X2021007_Francais_CSV_data.csv',
    "source_profil_sdr_qc": f'98-401-X2021020_Francais_CSV_data.csv',
    "profil_sdr_qc": f'profil_2021_sdr_qc.csv',
    "profil_sr_qc": f'profil_2021_sr_qc.csv',
    "eq_sr_sdr_sm": f'eq_sr_sdr_sm'
}
# dictionnaire des listes de variables
varlists = {
    "varlist_rename": {
        "NIVEAU_GÉO": "TYPE_GEO",
        "ID_CARACTÉRISTIQUE": "MODALITE_ID",
        "NOM_CARACTÉRISTIQUE": "MODALITE_NOM",
        "C1_CHIFFRE_TOTAL": "TOTAL",
        "C2_CHIFFRE_HOMMES+": "H",
        "C3_CHIFFRE_FEMMES+": "F",
        "C10_TAUX_TOTAL": "TAUX_TOTAL",
        "C11_TAUX_HOMMES+": "TAUX_H",
        "C12_TAUX_FEMMES+": "TAUX_F"
    },
    "list_od": [
        "mtl", "que"
    ],
    "varlist_popagesex": [
        "TYPE_GEO", "GEO_CODE", "MODALITE_ID", "MODALITE_NOM", "H", "F"
    ],
    "varlist_taille_menage": [
        "TYPE_GEO", "GEO_CODE", "MODALITE_ID", "MODALITE_NOM", "TOTAL"
    ]
}
# dictionnaire des no.ID des variables à extraire
id_var = {
    "age": pd.Series(list(range(10, 13)) + list(range(14, 24))+list(range(25, 30))),
    "taille_menage": pd.Series(list(range(51, 56)))
}

dict_popagesex = {
    "input_long_sr": f'{paths["path_input_folder"]}popagesex_2021_SR_long.csv',
    "output_wide_sr": f'{paths["path_output_folder"]}popagesex_2021_SR_wide.csv',
    "input_long_sdr": f'{paths["path_input_folder"]}popagesex_2021_SDR_long.csv',
    "output_wide_sdr": f'{paths["path_input_folder"]}popagesex_2021_SDR_wide.csv',
    "pivot_vars": ['GEO_CODE', 'AGE'],
    "keep_vars": [],
    "dtype": {
        "GEO_CODE": "string",
        "H": "int",
        "F": "int",
        "AGE": "category"
    }
}
dict_taille_menage = {
    "input_long_sr": f'{paths["path_input_folder"]}taille_menage_2021_SR_long.csv',
    "output_wide_sr": f'{paths["path_output_folder"]}taille_menage_2021_SR_wide.csv',
    "input_long_sdr": f'{paths["path_input_folder"]}taille_menage_2021_SDR_long.csv',
    "output_wide_sdr": f'{paths["path_input_folder"]}taille_menage_2021_SDR_wide.csv',
    "pivot_vars": ['GEO_CODE', 'MODALITE_NOM'],
    "keep_vars": [],
    "dtype": {
        "GEO_CODE": "string",
        "TOTAL": "int",
    }
}

LIST_AGEGROUP_NAMES = pd.Series(['00_04', '05_09', '10_14', '15_19', '20_24', '25_29', '30_34', '35_39', '40_44',
                                '45_49', '50_54', '55_59', '60_64', '65_69', '70_74', '75_79', '80_84', '85_PL'])


clean_source_csv_sr = False  # Réexporter fichier profil source avec les variables pertinentes puis renommer.
clean_source_csv_sdr = False
export_long_sr = False  # Exporter la table renommée en format long avec sélection de types de géographie et de modalités.
export_long_sdr = False

# ----------------------------------------------------------------------------------------------------------------------
# ### FONCTIONS #################################################################################

def pop_age_sex_geo(input_df, type_geo_renommer):
    """

    :param input_df:
    :param type_geo:
    :return:
    """
    # Extraire les groupes d'âge
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["age"]
    )
    # Renommer
    df['AGE'] = np.tile(
        LIST_AGEGROUP_NAMES, len(df) // len(LIST_AGEGROUP_NAMES) + 1)[:len(df)]
    df['TYPE_GEO'] = type_geo_renommer
    df = (
        df
        .drop(columns=["TYPE_GEO", "MODALITE_ID",  "MODALITE_NOM"])
        .fillna(0)
    )
    return df


def taille_menages(input_df, type_geo_renommer):
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["taille_menage"]
    )
    df['TYPE_GEO'] = type_geo_renommer
    df = (
        df
        .drop(columns=["TYPE_GEO", "MODALITE_ID", "H", "F", "TAUX_TOTAL", "TAUX_H", "TAUX_F"])
        .fillna(0)
    )
    return df


def export_wide_csv(input_df, export_filename):
    input_df.to_csv(
        export_filename,
        sep=";",
        encoding='utf-8-sig',
        index=False
    )

# ----------------------------------------------------------------------------------------------------------------------
# ### MAIN ####################################################


# filtrer le fichier source de StatCan et l'exporter avec les Sr du qc seulement puis renommer les colonnes.
if clean_source_csv_sr:
    # Extraire les SR
    varlist = {"NOM_GÉO": "GEO_CODE"}
    varlist.update(varlists["varlist_rename"])
    clean_sr(
        input_path=f'{paths["path_input_folder"]}{paths["profil_rmr_ar_sr"]}',
        output_path=f'{paths["path_input_folder"]}{paths["profil_sr_qc"]}',
        dict_champs=varlist
    )

if clean_source_csv_sdr:
    varlist = {"CODE_GÉO_ALT": "GEO_CODE"}
    varlist.update(varlists["varlist_rename"])
    clean_sdr(
        input_path=f'{paths["path_input_folder"]}{paths["source_profil_sdr_qc"]}',
        output_path=f'{paths["path_input_folder"]}{paths["profil_sdr_qc"]}',
        dict_champs=varlist
    )

if export_long_sr:
    full_df = pd.read_csv(
        f'{paths["path_input_folder"]}{paths["profil_sr_qc"]}',
        sep=";",
        dtype={
            "GEO_CODE": "string"
        }
    )
    df_popagesex_sr = pop_age_sex_geo(full_df, "SR")
    df_popagesex_sr.to_csv(
        f'{dict_popagesex["input_long_sr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
    df_taille_menage_sr = taille_menages(full_df, "SR")
    df_taille_menage_sr.to_csv(
        f'{dict_taille_menage["input_long_sr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )

if export_long_sdr:
    full_df = pd.read_csv(
        f'{paths["path_input_folder"]}{paths["profil_sdr_qc"]}',
        sep=";",
        dtype={
            "GEO_CODE": "string"
        }
    )
    df_popagesex_sdr = pop_age_sex_geo(full_df, "SDR")
    df_popagesex_sdr.to_csv(
        f'{dict_popagesex["input_long_sdr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
    df_taille_menage_sdr = taille_menages(full_df, "SDR")
    df_taille_menage_sdr.to_csv(
        f'{dict_taille_menage["input_long_sdr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )

wide_popagesexe_sr = ltw(
    pd.read_csv(
        filepath_or_buffer=dict_popagesex["input_long_sr"],
        sep=";",
        dtype=dict_popagesex["dtype"]
    ),
    dict_popagesex["pivot_vars"]
)
wide_taille_menage_sr = ltw(
    pd.read_csv(
        filepath_or_buffer=dict_taille_menage["input_long_sr"],
        sep=";",
        dtype=dict_taille_menage["dtype"]
    ),
    dict_taille_menage["pivot_vars"]
)
wide_popagesexe_sdr = ltw(
    pd.read_csv(
        filepath_or_buffer=dict_popagesex["input_long_sdr"],
        sep=";",
        dtype=dict_popagesex["dtype"]
    ),
    dict_popagesex["pivot_vars"]
)
wide_taille_menage_sdr = ltw(
    pd.read_csv(
        filepath_or_buffer=dict_taille_menage["input_long_sdr"],
        sep=";",
        dtype=dict_taille_menage["dtype"]
    ),
    dict_taille_menage["pivot_vars"]
)

# joindre les df par sr et sdr
wide_popagesexe = (
    pd
    .concat([wide_popagesexe_sr, wide_popagesexe_sdr], axis=0)
    .set_index(keys='GEO_CODE', drop=False)
)
wide_taille_menage = (
    pd
    .concat([wide_taille_menage_sr, wide_taille_menage_sdr], axis=0)
    .set_index(keys='GEO_CODE', drop=False)
)

# Importer les dictionnaires d'équivalence SR/SDR par SM
df_final = pd.DataFrame()
for od in varlists["list_od"]:
    df = pd.read_csv(
        filepath_or_buffer=f'{paths["path_input_folder"]}{paths["eq_sr_sdr_sm"]}_{od}.csv',
        sep=",",
        dtype={"SRIDU": "string", "SDRIDU": "string"}
    )
    if 'PROPPOPSM' not in df.columns:
        df['PROPPOPSM'] = 1
    df['ID'] = np.where(df['SRIDU'].isna(), df['SDRIDU'], df['SRIDU'])
    df['OD'] = od
    #TODO : corriger la colonne SM
    df = df.set_index(keys='ID', drop=False)
    df_final = pd.concat([df_final, df], axis=0)

pass

