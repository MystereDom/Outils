import pandas as pd
from import_csv import import_csv
import parametres as params

paths = {
    "ivt_source": "S:/Donnees_SocioDemo/StatistiqueCanada/Recensement2021/CO-2535_Demographie/",
    "2011": "S:/Donnees_SocioDemo/StatistiqueCanada/Recensement2011/"
}

# import .csv
df_popagesex_2011 = import_csv(f'{paths["ivt_source"]}Pop_age_sex_menages_prives_2011_zp.csv')
# enlever la premiere rangée
df_popagesex_2011 = df_popagesex_2011.loc[1:, :]

# renommer les colonnes
field_names = params.LIST_AGEGROUP_NAMES
pre_t = ["T" + sub for sub in field_names]
pre_h = ["H" + sub for sub in field_names]
pre_f = ["F" + sub for sub in field_names]
field_names = ["ZP_NO", "GEO_TYPE", "T_TOT", *pre_t, "H_TOT", *pre_h, "F_TOT", *pre_f]
df_popagesex_2011 = df_popagesex_2011.set_axis(field_names, axis=1)

# changer les datatype
df_popagesex_2011[["ZP_NO", "T_TOT", *pre_t, "H_TOT", *pre_h, "F_TOT", *pre_f]] = \
    df_popagesex_2011[["ZP_NO", "T_TOT", *pre_t, "H_TOT", *pre_h, "F_TOT", *pre_f]].astype(int)
df_popagesex_2011["GEO_TYPE"] = df_popagesex_2011["GEO_TYPE"].astype("category")

# Correction des ZP
# ZP_NO 30900 (ZP b50001) = ZP_NO 309 - SDR 2449130
# ZP_NO 30901 (ZP c49015) = SDR 2449130
# ZP_NO 37001 (ZP b19003) = ZP_NO 37 + SDR 2419090
# ZP_NO 38001 (ZP c19004) = ZP_NO 38 - SDR 2419090
df_sdr = import_csv(f'{paths["2011"]}2011_SDR19090_et_49130.csv')
sdr_49130 = df_sdr[df_sdr["SDR"] == 49130].iloc[0, 1:]
zp_30900 = df_popagesex_2011[df_popagesex_2011["ZP_NO"] == 309][[*pre_t, *pre_h, *pre_f]] - sdr_49130
zp_30901 = sdr_49130.to_frame().T
sdr_19090 = df_sdr[df_sdr["SDR"] == 19090].iloc[0, 1:]
zp_37001 = df_popagesex_2011[df_popagesex_2011["ZP_NO"] == 37][[*pre_t, *pre_h, *pre_f]] + sdr_19090
zp_38001 = df_popagesex_2011[df_popagesex_2011["ZP_NO"] == 38][[*pre_t, *pre_h, *pre_f]] - sdr_19090
zp_modif = pd.concat([zp_30900, zp_30901, zp_37001, zp_38001])
zp_modif["ZP_NO"] = [30900, 30901, 37001, 38001]
zp_modif = zp_modif.astype(int)
zp_modif["GEO_TYPE"] = ["Groupe_SDR", "SDR", "Group_SDR", "SDR"]
zp_modif["T_TOT"] = zp_modif[[*pre_t]].sum(axis=1)
zp_modif["H_TOT"] = zp_modif[[*pre_h]].sum(axis=1)
zp_modif["F_TOT"] = zp_modif[[*pre_f]].sum(axis=1)

# Ajouter les modifications au df
df_popagesex_2011 = pd.concat([df_popagesex_2011, zp_modif])
df_popagesex_2011 = df_popagesex_2011[~(df_popagesex_2011["ZP_NO"].isin([37, 38, 309]))]

# Ajouter la colonne ANNEE
df_popagesex_2011["ANNEE"] = 2011

# Ajouter la colonne NOM
# 1. Importer le dicitonnaire
eq_no_nom = (
    import_csv(f'{paths["ivt_source"]}eq_zpno_zpnomzp.csv')
    .astype({"ZP_NO": int, "NOM": str})
    .set_index("ZP_NO")
    .to_dict()["NOM"]
)
df_popagesex_2011['NOM'] = df_popagesex_2011['ZP_NO'].map(eq_no_nom)

# Remettre les colonnes en ordre
df_popagesex_2011 = df_popagesex_2011[["ZP_NO", "NOM", "ANNEE", "T_TOT", *pre_t, "H_TOT", *pre_h, "F_TOT", *pre_f]]

# Exporter en csv
df_popagesex_2011.to_csv(f'{paths["2011"]}popagesex_2011_zp21_final.csv')
pass

# Ajouter le champs de nom de ZP

# Ajouter l'année?
