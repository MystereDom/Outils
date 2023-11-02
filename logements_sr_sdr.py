from join_tables import join_tables as jt
from join_tables import import_csv as import_csv
import pandas as pd

# ajouter le champs ZP a la table de données de logements par SR
df_sr = jt(
    input1=f'S:/Donnees_SocioDemo/StatistiqueCanada/Recensement2021/Logements_2021/Logements_2021_SR.csv',
    input2=f'X:/_Dominic/Projets en cours/ES3/Geo/dictionnaires_equivalence/eq_zp_sr_2016.csv',
    join_field="SR"
)
df_sr = df_sr[~df_sr["ZP"].isna()]
df_sr_logements = df_sr[["SR", "LOG2021", "MEN2021"]].to_csv(
    f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_2021_par_ZP_SR.csv',
    sep=";",
    index=False
)
df_logements_par_zp_sr = (
    df_sr[["LOG2021", "MEN2021", "ZP"]]
    .groupby('ZP')
    .sum()
    .to_csv(
        f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_2021_par_ZP_SR.csv',
        sep=";",
        index=True
    )
)

# Ajouter les ZP à la table des logements 2021 par SDR
df_sdr = jt(
    input1=f'S:/Donnees_SocioDemo/StatistiqueCanada/Recensement2021/Logements_2021/Logements_2021_SDR.csv',
    input2=f'X:/_Dominic/Projets en cours/ES3/Geo/dictionnaires_equivalence/eq_zp_sdr_2016.csv',
    join_field="SDR"
)
df_sdr = df_sdr[~df_sdr["ZP"].isna()]
df_logements_par_zp_sdr = (
    df_sdr[["ZP", "LOG2021", "MEN2021"]]
    .groupby('ZP')
    .sum()
    .to_csv(
        f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_2021_par_ZP_SDR.csv',
        sep=";",
        index=True
    )
)

df_logements = (
    pd
    .concat([
        import_csv(f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_2021_par_ZP_SDR.csv'),
        import_csv(f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_2021_par_ZP_SR.csv')
    ])
    .to_csv(
        f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_et_menages_par_ZP_2021.csv',
        sep=";",
        index=False
    )
)

df = jt(
    input1=f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_et_menages_par_ZP_2006_2016.csv',
    input2=f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_et_menages_par_ZP_2021.csv',
    join_field="ZP"
)
df.to_csv(
    f'X:/_Dominic/Projets en cours/Test_Hamilton_Perry/Logements_et_menages_par_ZP_2006_2021.csv',
    sep=";",
    index=False
)
pass