# Outil pour extraire les données pertinentes du fichier de profil du recensement.
import csv
import pandas as pd
import numpy as np
from long_to_wide import long_to_wide as ltw
from extract_by_value import extract_by_value as ebv

# PARAMS ########
paths = {
    "path_rmr_ar_sr_input": f'C:/Projets_Python/Outils_Demo/inputs/',
    "path_rmr_ar_sr_output": f'C:/Projets_Python/Outils_Demo/outputs/',
    "profil_rmr_ar_sr": f'98-401-X2021007_Francais_CSV_data.csv',
    "profil_varlist_pertinentes": f'full_profile_var_pertinentes_2021.csv'
}
varlists = {
    "varlist_full_pertinentes": [
        "NIVEAU_GÉO", "NOM_GÉO", "ID_CARACTÉRISTIQUE", "NOM_CARACTÉRISTIQUE", "C1_CHIFFRE_TOTAL", "C2_CHIFFRE_HOMMES+",
        "C3_CHIFFRE_FEMMES+", "C10_TAUX_TOTAL", "C11_TAUX_HOMMES+", "C12_TAUX_FEMMES+"
    ],
    "varlist_rename": [
        "GEO", "GEO_CODE", "MODALITE_ID", "MODALITE_NOM", "TOTAL", "H", "F", "TAUX_TOTAL", "TAUX_H", "TAUX_F"
    ],
    "varlist_popagesex": [
        "GEO", "GEO_CODE", "MODALITE_ID", "MODALITE_NOM", "H", "F"
    ],
    "varlist_taille_menage": [
        "GEO", "GEO_CODE", "MODALITE_ID", "MODALITE_NOM", "TOTAL"
    ]
}
id_var = {
    "age": pd.Series(list(range(10, 13)) + list(range(14, 24))+list(range(25, 30))),
    "taille_menage": pd.Series(list(range(51, 56)))
}
# Faire une classe?
dict_popagesex = {
    "input_long": f'{paths["path_rmr_ar_sr_input"]}popagesex_2021_SR_long.csv',
    "output_wide": f'{paths["path_rmr_ar_sr_output"]}popagesex_2021_SR_wide.csv',
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
    "input_long": f'{paths["path_rmr_ar_sr_input"]}taille_meange_2021_SR_long.csv',
    "output_wide": f'{paths["path_rmr_ar_sr_output"]}taille_menage_2021_SR_wide.csv',
    "pivot_vars": ['GEO_CODE', 'MODALITE_NOM'],
    "keep_vars": [],
    "dtype": {
        "GEO_CODE": "string",
        "TOTAL": "int",
    }
}

var_pertinentes_bool = False  # Réexporter la table initiale avec les variables pertinentes. Variables renommées.
export_long = True  # Exporter la table renommée en format long avec sélection de géographie et de modalités.

LIST_AGEGROUP_NAMES = pd.Series(['00_04', '05_09', '10_14', '15_19', '20_24', '25_29', '30_34', '35_39', '40_44',
                                '45_49', '50_54', '55_59', '60_64', '65_69', '70_74', '75_79', '80_84', '85_PL'])


# ### FONCTIONS ##############################################
def clean_full_table(var_pertinentes):
    df = pd.read_csv(
        filepath_or_buffer=f'{paths["path_rmr_ar_sr_input"]}{paths["profil_rmr_ar_sr"]}',
        usecols=var_pertinentes,
        sep=",",
        dtype={
            "NOM_GÉO": "string"
        }
        )
    df = df.set_axis(varlists["varlist_rename"], axis=1)
    df.to_csv(
        f'C:/Projets_Python/Outils_Demo/inputs/full_profile_var_pertinentes_2021.csv',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )


def extract_data(input_df, varlist, geo):
    """

    :param input_df:
    :param varlist:
    :param geo:
    :return:
    """
    df = input_df[varlist]
    df = df[df["GEO"] == geo].fillna(0)
    return df


def pop_age_sex_geo(input_df, geo):
    """

    :param input_df:
    :param geo:
    :param nomgeo:
    :return:
    """
    df = input_df[(input_df["GEO"] == geo)]
    # Extraire les groupes d'âge
    df = ebv(df, "MODALITE_ID", id_var["age"])
    df['AGE'] = np.tile(
        LIST_AGEGROUP_NAMES, len(df) // len(LIST_AGEGROUP_NAMES) + 1)[:len(df)]
    df = df.drop(columns=["GEO", "MODALITE_ID",  "MODALITE_NOM"])
    return df


def taille_menages(input_df, geo):
    # Filtrer par type de découpage géographique
    df = input_df[(input_df["GEO"] == geo)]
    df = ebv(df, "MODALITE_ID", id_var["taille_menage"])
    df = df.drop(columns=["GEO", "MODALITE_ID"])
    return df


def long_to_wide_export(input_name, export_name, pivot_vars, dtypes):
    df_long = pd.read_csv(
        filepath_or_buffer=input_name,
        sep=";",
        dtype=dtypes
    )
    # long to wide
    df_wide = ltw(df_long, pivot_vars)
    df_wide.to_csv(
        export_name,
        sep=";",
        encoding='utf-8-sig',
        index=False
    )

# ### MAIN ####################################################

if var_pertinentes_bool:
    clean_full_table(varlists["varlist_full_pertinentes"])

if export_long:
    full_df = pd.read_csv(
        f'{paths["path_rmr_ar_sr_input"]}{paths["profil_varlist_pertinentes"]}',
        sep=";",
        dtype={
            "NOM_GÉO": "string"
        }
    )
    # Extraire les champs pertinents seulement.
    df_popagesex_sr = extract_data(full_df, varlists["varlist_popagesex"], "Secteur de recensement")
    df_popagesex_sr = pop_age_sex_geo(df_popagesex_sr, "Secteur de recensement")
    df_popagesex_sr.to_csv(
        f'{dict_popagesex["input_long"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
    df_taille_menage_sr = extract_data(full_df, varlists["varlist_taille_menage"], "Secteur de recensement")
    df_taille_menage_sr = taille_menages(df_taille_menage_sr, "Secteur de recensement")
    df_taille_menage_sr.to_csv(
        f'{dict_taille_menage["input_long"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )

long_to_wide_export(
    dict_popagesex["input_long"],
    dict_popagesex["output_wide"],
    dict_popagesex["pivot_vars"],
    dict_popagesex["dtype"]
)
long_to_wide_export(
    dict_taille_menage["input_long"],
    dict_taille_menage["output_wide"],
    dict_taille_menage["pivot_vars"],
    dict_taille_menage["dtype"]
)
pass

