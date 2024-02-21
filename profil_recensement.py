# Outil pour traiter le fichier source du fichier de profil du recensement.
# Extraction des variables pertinentes.
# Les fichiers de profil du recensement sont produits par type de géographie.
# Les SR et les SDR ne sont pas dans le même fichier
# Le profil de 2011 ne contient pas de champs de ID pour les modalités. (472 modalités)
# Production des tables en format wide avec sommations par SM

import pandas as pd
import numpy as np
from long_to_wide import long_to_wide as ltw
from extract_by_value import extract_by_value as ebv
from profil_long_extract_vars import export_extract_long as long
# from profil_long_extract_vars import export_extract_long_sdr as long_sdr

# ----------------------------------------------------------------------------------------------------------------------
# ### PARAMS

params = {
    "annee_recensement": f'2021',
    "variables_a_extraire": ["age", "taille_menages", "nb_logements", "nb_menages"]
}
# dictionnaire des chemins d'accès
paths = {
    "input_folder": f'C:/Projets_Python/Outils_Demo/inputs/',
    "output_folder": f'C:/Projets_Python/Outils_Demo/outputs/',
    "profil_sdr_qc": f'profil_{params["annee_recensement"]}_sdr_qc.csv',
    "profil_sr_qc": f'profil_{params["annee_recensement"]}_sr_qc.csv',
    "eq_sr_sdr_sm": f'eq_sr_sdr_sm'
}
# dictionnaire des listes de variables
varlists = {
    "list_od": ["mtl", "que"],
    "colkeep": {
        "extract": ["GEO_CODE", "MODALITE_NOM", "MODALITE_ID", "TOTAL", "H", "F"],
        "popagesex": ["GEO_CODE", "H", "F", "AGE"],
        "taille_menage": ["GEO_CODE", "MODALITE_NOM", "TOTAL"],
        "nb_logements": ["GEO_CODE", "TOTAL"],
        "nb_menages": ["GEO_CODE", "TOTAL"]
    }
}
var_dtypes = {
    "GEO_CODE": "string",
    "MODALITE_NOM": "category",
    "MODALITE_ID": "int16",
    "TOTAL": "int",
    "H": "int",
    "F": "int"
}
# dictionnaire des no.ID des variables à extraire
id_var = {
    "2021": {
        "age": pd.Series(list(range(10, 13)) + list(range(14, 24))+list(range(25, 30))),
        "taille_menage": pd.Series(list(range(51, 56))),
        "nb_logements": pd.Series([4]),
        "nb_menages": pd.Series([5])
    },
    "2016": {
        "age": pd.Series(list(range(10, 13)) + list(range(14, 24))+list(range(25, 30))),
        "taille_menage": pd.Series(list(range(51, 56))),
        "nb_logements": pd.Series([4]),
        "nb_menages": pd.Series([5])
    },
    "2011": {
        "age": pd.Series(list(range(9, 13)) + list(range(18, 32))),
        "taille_menage": pd.Series(list(range(119, 125))),
        "nb_logements": pd.Series([4]),
        "nb_menages": pd.Series([5])
    },
    "2006": {
        "age": pd.Series(list(range(9, 13)) + list(range(18, 32))),
        "taille_menage": pd.Series(list(range(119, 125))),
        "nb_logements": pd.Series([4]),
        "nb_menages": pd.Series([5])
    }
}

dict_popagesex = {
    "pivot_vars": ['GEO_CODE', 'AGE'],
    "keep_vars": [],
    "dtype": {
        "GEO_CODE": "string",
        "TOTAL": "int",
        "H": "int",
        "F": "int"
    }
}
dict_taille_menage = {
    "pivot_vars": ['GEO_CODE', 'MODALITE_NOM'],
    "keep_vars": [],
    "dtype": {
        "GEO_CODE": "string",
        "TOTAL": "int",
    }
}
# TODO : distionnaire d'eq des SR/SDR par année
# liste de nom des groupes d'âge
LIST_AGEGROUP_NAMES = pd.Series(['00_04', '05_09', '10_14', '15_19', '20_24', '25_29', '30_34', '35_39', '40_44',
                                '45_49', '50_54', '55_59', '60_64', '65_69', '70_74', '75_79', '80_84', '85_PL'])

# Exporter la table renommée en long avec sélection de types de géographie et de modalités.
export_long = True

# ----------------------------------------------------------------------------------------------------------------------
# ### FONCTIONS #################################################################################


def pop_age_sex_geo(input_df):
    """
    Extraction des variables de populaiton par groupe d'âge et sexe
    :param input_df:
    :return:
    """
    # Extraire les groupes d'âge
    a = list(id_var[params["annee_recensement"]]["age"])
    input_df = input_df[input_df["MODALITE_ID"].isin(list(id_var[params["annee_recensement"]]["age"]))]
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var[params["annee_recensement"]]["age"]
    ).drop(["MODALITE_ID", "MODALITE_NOM"], axis=1)
    # Créer la colonne des groupes d'âge
    df['AGE'] = np.tile(
        LIST_AGEGROUP_NAMES, len(df) // len(LIST_AGEGROUP_NAMES) + 1)[:len(df)]
    return df


def taille_menages(input_df, type_geo_renommer):
    df = (
        ebv(
            input_df=input_df,
            colname="MODALITE_ID",
            varvalue=id_var[params["annee_recensement"]]["taille_menage"]
        )
        .drop(["MODALITE_ID", "H", "F"], axis=1)
        .rename({"TOTAL": f'TAILLE_MEN{params["annee_recensement"]}'}, axis=1)
    )
    df['TYPE_GEO'] = type_geo_renommer
    return df


def nb_logements(input_df):
    df = (
        ebv(
            input_df=input_df,
            colname="MODALITE_ID",
            varvalue=id_var[params["annee_recensement"]]["nb_logements"]
        )
        .drop(["MODALITE_ID", "MODALITE_NOM", "H", "F"], axis=1)
        .rename({"TOTAL": f'LOG{params["annee_recensement"]}'}, axis=1)
    )
    return df


def nb_menages(input_df):
    df = (
        ebv(
            input_df=input_df,
            colname="MODALITE_ID",
            varvalue=id_var[params["annee_recensement"]]["nb_menages"]
        )
        .drop(["MODALITE_ID", "MODALITE_NOM", "H", "F"], axis=1)
        .rename({"TOTAL": f'MEN{params["annee_recensement"]}'}, axis=1)
    )
    return df


def export_wide_csv(input_df, export_filename):
    input_df.to_csv(
        export_filename,
        sep=";",
        encoding='utf-8-sig',
        index=False
    )


def sm_od(df, eq_df):
    df["OD"] = (
        df
        .index
        .values
    )
    df["OD"] = df["OD"].map(dict(zip(eq_df.SM, eq_df.OD)))
    return df


# ----------------------------------------------------------------------------------------------------------------------
# ### MAIN ####################################################


# filtrer le fichier source de StatCan et l'exporter avec les SR et SDR du qc seulement puis renommer les colonnes.
if export_long:
    sr_long = (
        long(
            path=f'{paths["input_folder"]}profil_{params["annee_recensement"]}_sr_qc.csv',
            vars_to_extract=id_var,
            colkeep=varlists["colkeep"]["extract"],
            var_dtypes=var_dtypes
        )
    )
    sr_long = (
        long(
            path=f'{paths["input_folder"]}profil_{params["annee_recensement"]}_sr_qc.csv',
            vars_to_extract=id_var[params["annee_recensement"]],
            colkeep=varlists["colkeep"]["extract"],
            var_dtypes=var_dtypes
        )
        .to_csv(
            f'{paths["output_folder"]}extract_{params["annee_recensement"]}_SR_long.csv',
            sep=";",
            encoding='utf-8-sig',
            index=False
        )
    )
    sdr_long = (
        long(
            path=f'{paths["input_folder"]}profil_{params["annee_recensement"]}_sdr_qc.csv',
            vars_to_extract=id_var[params["annee_recensement"]],
            colkeep=varlists["colkeep"]["extract"],
            var_dtypes=var_dtypes
        )
        .to_csv(
            f'{paths["output_folder"]}extract_{params["annee_recensement"]}_SDR_long.csv',
            sep=";",
            encoding='utf-8-sig',
            index=False
        )
    )

# importer les Dataframe en format long
df_long_sr = pd.read_csv(
    filepath_or_buffer=f'{paths["output_folder"]}extract_{params["annee_recensement"]}_SR_long.csv',
    sep=";",
    dtype=var_dtypes
)
df_long_sdr = pd.read_csv(
    filepath_or_buffer=f'{paths["output_folder"]}extract_{params["annee_recensement"]}_SDR_long.csv',
    sep=";",
    dtype=var_dtypes
)
# Extraire un dataframe de la pop_age_age_sex
df_long_pop_age_sex_sr = pop_age_sex_geo(df_long_sr)
df_long_pop_age_sex_sdr = pop_age_sex_geo(df_long_sdr)
df_long_taille_menages_sr = taille_menages(df_long_sr, "SR")
df_long_taille_menages_sdr = taille_menages(df_long_sdr, "SDR")
df_logements = (
    pd
    .concat([nb_logements(df_long_sr), nb_logements(df_long_sdr)], axis=0)
    .set_index(keys='GEO_CODE', drop=False)
)
df_menages = (
    pd
    .concat([nb_menages(df_long_sr), nb_menages(df_long_sdr)], axis=0)
    .set_index(keys='GEO_CODE', drop=False)
)
df_long_menages_sr = nb_menages(df_long_sr)
df_long_menages_sdr = nb_menages(df_long_sdr)

# Transformer les DataFrame en format wide
wide_popagesexe_sr = ltw(df_long_pop_age_sex_sr, dict_popagesex["pivot_vars"])
wide_taille_menage_sr = ltw(df_long_taille_menages_sr, dict_taille_menage["pivot_vars"])
wide_popagesexe_sdr = ltw(df_long_pop_age_sex_sdr, dict_popagesex["pivot_vars"])
wide_taille_menage_sdr = ltw(df_long_taille_menages_sdr, dict_taille_menage["pivot_vars"])

# Joindre les df par sr et sdr
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

# Renommer les index
wide_popagesexe.index.names = ["ID"]
wide_taille_menage.index.names = ["ID"]
df_logements.index.names = ["ID"]
df_menages.index.names = ["ID"]

# Importer les dictionnaires d'équivalence SR/SDR par SM
df_eq_final = pd.DataFrame()
for od in varlists["list_od"]:
    df_eq_sr_sdr_sm_od = pd.read_csv(
        filepath_or_buffer=f'{paths["input_folder"]}{paths["eq_sr_sdr_sm"]}_{od}.csv',
        sep=",",
        dtype={"SRIDU": "string", "SDRIDU": "string"}
    )
    if 'PROPPOPSM' not in df_eq_sr_sdr_sm_od.columns:
        df_eq_sr_sdr_sm_od['PROPPOPSM'] = 1
    df_eq_sr_sdr_sm_od['ID'] = np.where(
        df_eq_sr_sdr_sm_od['SRIDU'].isna(),
        df_eq_sr_sdr_sm_od['SDRIDU'],
        df_eq_sr_sdr_sm_od['SRIDU']
    )
    df_eq_sr_sdr_sm_od['OD'] = od
    df_eq_sr_sdr_sm_od = (
        df_eq_sr_sdr_sm_od
        .set_index(keys='ID', drop=False)
        .drop(columns=["SRIDU", "SDRIDU", "TYPD_COUP"])
    )
    df_eq_final = pd.concat([df_eq_final, df_eq_sr_sdr_sm_od], axis=0)
df_eq_final.set_index(keys="ID")

# Merger les données de population et de taille de ménage avec le dictionnaire d'équivalences SR/SDR/SM
wide_popagesexe = df_eq_final.join(wide_popagesexe, how='left')
wide_taille_menage = df_eq_final.join(wide_taille_menage, how='left')

# Appliquer la pondération
# 1. Créer une liste des champs auxquels on applique la pondération.
weighted_field_list = [col for col in wide_popagesexe if col.startswith("H") or col.startswith("F")]
weighted_fields = wide_popagesexe[weighted_field_list].apply(lambda x: round(x*wide_popagesexe['PROPPOPSM'], 0))
wide_popagesexe[weighted_field_list] = weighted_fields
# 2. Taille des ménages
weighted_field_list = [col for col in wide_taille_menage if col.startswith("TOTAL")]
weighted_fields = wide_taille_menage[weighted_field_list].apply(lambda x: round(x*wide_taille_menage['PROPPOPSM'], 0))
wide_taille_menage[weighted_field_list] = weighted_fields

# Sommer par SM
sm_popagesex = (
    wide_popagesexe
    .groupby(by='SM')
    .sum()
    .drop(['PROPPOPSM', 'ID', 'OD', 'GEO_CODE'], axis=1)
)
a = wide_popagesexe[wide_popagesexe["SM"] == 120]

sm_taille_menages = (
    wide_taille_menage
    .groupby(by='SM')
    .sum()
    .drop(['PROPPOPSM', 'ID', 'OD', 'GEO_CODE'], axis=1)
)

# Appliquer le champs "OD"
# 1. Créer une table d'équivalence SM-OD
eq_sm_od = (
    df_eq_final[['SM', 'OD']]
    .drop_duplicates()
)
# 2. Ajouter la colonne "OD"
sm_popagesex = sm_od(sm_popagesex, eq_sm_od)
sm_taille_menages = sm_od(sm_taille_menages, eq_sm_od)

# Exporter en csv
sm_popagesex.to_csv(
    f'{paths["output_folder"]}population_par_age_sexe_SM_{params["annee_recensement"]}.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)
sm_taille_menages.to_csv(
    f'{paths["output_folder"]}taille_menages_SM_{params["annee_recensement"]}.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)
df_logements.to_csv(
    f'{paths["output_folder"]}logements_{params["annee_recensement"]}.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)
df_menages.to_csv(
    f'{paths["output_folder"]}menages_{params["annee_recensement"]}.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)

pass
