# Outil pour extraire les données pertinentes du fichier de profil du recensement.

import pandas as pd
import numpy as np
from long_to_wide import long_to_wide as ltw
from extract_by_value import extract_by_value as ebv

# ----------------------------------------------------------------------------------------------------------------------
# ### PARAMS
# dictionnaire des chemins d'accès
params = {
    "annee_recensement": f'2021',
    "variables_a_extraire": ["age", "taille_menages", "nb_logements", "nb_menages"]
}

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
    "list_od": [
        "mtl", "que"
    ],
    "keepvars_popagesex": [
       "GEO_CODE", "H", "F", "AGE"
    ],
    "keepvars_taille_menage": [
       "GEO_CODE", "MODALITE_NOM", "TOTAL"
    ]
}
# dictionnaire des no.ID des variables à extraire
id_var = {
    "age": pd.Series(list(range(10, 13)) + list(range(14, 24))+list(range(25, 30))),
    "taille_menage": pd.Series(list(range(51, 56))),
    "nb_logements": pd.Series([4]),
    "nb_menages": pd.Series([5])
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
export_long_sr = False  # Exporter la table renommée en long avec sélection de types de géographie et de modalités.
export_long_sdr = False

# ----------------------------------------------------------------------------------------------------------------------
# ### FONCTIONS #################################################################################


def pop_age_sex_geo(input_df, keepvars):
    """

    :param input_df:
    :param keepvars: Liste de colonnes à conserver
    :return:
    """
    # Extraire les groupes d'âge
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["age"]
    )
    # Créer la colonne des groupes d'âge
    df['AGE'] = np.tile(
        LIST_AGEGROUP_NAMES, len(df) // len(LIST_AGEGROUP_NAMES) + 1)[:len(df)]
    # Ne gader que les colonnes pertinentes
    df = df[keepvars].fillna(0)
    return df


def taille_menages(input_df, type_geo_renommer, keepvars):
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["taille_menage"]
    )
    df['TYPE_GEO'] = type_geo_renommer
    df = df[keepvars].fillna(0)
    return df


def nb_logements(input_df, keepvars):
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["nb_logements"]
    )
    df = df[keepvars].fillna(0)
    return df


def nb_menages(input_df, keepvars):
    df = ebv(
        input_df=input_df,
        colname="MODALITE_ID",
        varvalue=id_var["nb_menages"]
    )
    df = df[keepvars].fillna(0)
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


# filtrer le fichier source de StatCan et l'exporter avec les Sr du qc seulement puis renommer les colonnes.
if clean_source_csv_sr:
    # Extraire les SR
    varlist = {"NOM_GÉO": "GEO_CODE"}
    varlist.update(varlists["varlist_rename"])
    clean_sr(
        input_path=f'{paths["path_input_folder"]}{paths["profil_rmr_ar_sr"]}',
        output_path=f'{paths["path_input_folder"]}{paths["profil_sr_qc"]}',
        dict_rename=varlist
    )

if clean_source_csv_sdr:
    varlist = {"CODE_GÉO_ALT": "GEO_CODE"}
    varlist.update(varlists["varlist_rename"])
    clean_sdr(
        input_path=f'{paths["path_input_folder"]}{paths["source_profil_sdr_qc"]}',
        output_path=f'{paths["path_input_folder"]}{paths["profil_sdr_qc"]}',
        dict_rename=varlist
    )

if export_long_sr:
    full_df = pd.read_csv(
        f'{paths["path_input_folder"]}{paths["profil_sr_qc"]}',
        sep=";",
        dtype={
            "GEO_CODE": "string"
        }
    )
    df_popagesex_sr = pop_age_sex_geo(full_df, varlists["keepvars_popagesex"])
    df_popagesex_sr.to_csv(
        f'{dict_popagesex["input_long_sr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
    df_taille_menage_sr = taille_menages(full_df, "SR", varlists["keepvars_taille_menage"])
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
    df_popagesex_sdr = pop_age_sex_geo(full_df, varlists["keepvars_popagesex"])
    df_popagesex_sdr.to_csv(
        f'{dict_popagesex["input_long_sdr"]}',
        sep=";",
        encoding='utf-8-sig',
        index=False
    )
    df_taille_menage_sdr = taille_menages(full_df, "SDR", varlists["keepvars_taille_menage"])
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
# Renommer les index
wide_popagesexe.index.names = ['ID']
wide_taille_menage.index.names = ['ID']


# Importer les dictionnaires d'équivalence SR/SDR par SM
df_eq_final = pd.DataFrame()
for od in varlists["list_od"]:
    df_eq_sr_sdr_sm_od = pd.read_csv(
        filepath_or_buffer=f'{paths["path_input_folder"]}{paths["eq_sr_sdr_sm"]}_{od}.csv',
        sep=",",
        dtype={"SRIDU": "string", "SDRIDU": "string"}
    )
    if 'PROPPOPSM' not in df_eq_sr_sdr_sm_od.columns:
        df_eq_sr_sdr_sm_od['PROPPOPSM'] = 1
    df_eq_sr_sdr_sm_od['ID'] = np.where(
        df_eq_sr_sdr_sm_od['SRIDU'].isna(), df_eq_sr_sdr_sm_od['SDRIDU'], df_eq_sr_sdr_sm_od['SRIDU'])
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
    f'{paths["path_output_folder"]}population_par_age_sexe_SM.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)
sm_taille_menages.to_csv(
    f'{paths["path_output_folder"]}taille_menages_SM.csv',
    sep=";",
    encoding='utf-8-sig',
    index=True
)

pass
