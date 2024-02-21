# Création de tables des logements et ménages par SR et SDR pour les années 2006 à 2021.
# Les tables à importer sont produites à partir des profil du recensement.

from join_tables import join_tables as jt
from import_csv import import_csv as import_csv

# Initialisation du dictionnaire qui va contenir les DataFrame de logements et ménages par année.
df_par_zp_par_annee = {}

# TODO : Boucler sur les années
for year in ["2016", "2021"]:
    params = {"field_list": ["ZP", f'LOG{year}', f'MEN{year}']}
# Importer les tables de logements et de ménages puis joindre dans un DataFrame
# GEO_CODE as string parce que les SR ont parfois le format xxxxxxx.00
    df_logements = import_csv(
        f'C:/Projets_Python/Outils_Demo/outputs/logements_{year}.csv',
        {"GEO_CODE": "string"}
    ).drop("ID", axis=1)
    df_menages = import_csv(
        f'C:/Projets_Python/Outils_Demo/outputs/menages_{year}.csv',
        {"GEO_CODE": "string"}
    ).drop("ID", axis=1)
    df_log_men = jt(
        input1=df_logements,
        input2=df_menages,
        join_field="GEO_CODE"
    )

    # Importer les dictionnaires d'équivalence géographique SR/SDR vs ZP
    df_eq_sr = import_csv(
        f'X:/_Dominic/Projets en cours/ES3/Geo/dictionnaires_equivalence/eq_zp_sr_2016.csv',
        {"SR": "string"}
    ).rename({"SR": "GEO_CODE"}, axis=1)
    df_eq_sdr = import_csv(
        f'X:/_Dominic/Projets en cours/ES3/Geo/dictionnaires_equivalence/eq_zp_sdr_2016.csv',
        {"SDR": "string"}
    ).rename({"SDR": "GEO_CODE"}, axis=1)

    # ajouter la colonne ZP a la table de données de logements et menages par SR/SDR
    df_par_zp = jt(
        input1=df_log_men,
        input2=df_eq_sr,
        join_field="GEO_CODE"
    )
    df_par_zp = jt(
        input1=df_par_zp,
        input2=df_eq_sdr,
        join_field="GEO_CODE"
    )
    df_par_zp["ZP_x"] = df_par_zp["ZP_x"].fillna(df_par_zp["ZP_y"])
    df_par_zp = (df_par_zp
                 .rename({"ZP_x": "ZP"}, axis=1)
                 .drop("ZP_y", axis=1)
                 )
    df_par_zp = df_par_zp[~df_par_zp["ZP"].isna()].drop("GEO_CODE", axis=1)
    df_par_zp = df_par_zp[params["field_list"]]
    # TODO: sumby... ZP...
    df_par_zp_par_annee[f'{year}'] = df_par_zp

df = (
    df_par_zp_par_annee["2016"]
    .merge(
        right=df_par_zp_par_annee["2021"],
        on="ZP",
        how="left"
        )
    )
df.to_csv(
    f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_et_menages_par_ZP_test_2016_2021.csv',
    sep=";",
    index=False
)
pass