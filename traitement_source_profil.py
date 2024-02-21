# Outil pour traiter les fichiers source de profil de recensement par sr et sdr.
# Il faut que les fichiers source soient renommés sous le format "Fichier_source_profil_YYYY_SR/SDR.csv"
# Les fichiers de profil doivent être réexportés en UTF-8 avant d'âtre traités.

# TODO : Rendu à:
# Demander le Profil 2006 par SR

import pandas as pd
import numpy as np

# ----------------------------------------------------------------------------------------------------------------------
# PARAMS

params = {
    "annee": "2011",
    "working_directory": f'C:/Projets_Python/Outils_Demo/',
    "input_path": f'inputs/',
    "output_path": f'outputs/',
    "SR": True,
    "SDR": True
}

# code inscrit dans le fichier de profil qui permet d'identifier l'année et le type de découpage géographique

dict_varlist_rename_and_type = {
    "2021": {
        "SDR": {
            "CODE_GÉO_ALT": "GEO_CODE",
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
        "SDR_dtypes": {
            "CODE_GÉO_ALT": "string",
            "NIVEAU_GÉO": "category",
            "ID_CARACTÉRISTIQUE": "int16",
            "NOM_CARACTÉRISTIQUE": "category"
        },
        "SR": {
            "NOM_GÉO": "GEO_CODE",
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
        "SR_dtypes": {
            "NOM_GÉO": "string",
            "NIVEAU_GÉO": "category",
            "ID_CARACTÉRISTIQUE": "int16",
            "NOM_CARACTÉRISTIQUE": "category"
        }
    },
    "2016": {
        "SDR": {
            "CODE_GÉO_ALT": "GEO_CODE",
            "NIVEAU_GÉO": "TYPE_GEO",
            "Membre ID: Profil des aires de diffusion (57)": "MODALITE_ID",
            "DIM: Profil des aires de diffusion (57)": "MODALITE_NOM",
            "Dim: Sexe (3): Membre ID: [1]: Total - Sexe": "TOTAL",
            "Dim: Sexe (3): Membre ID: [2]: Sexe masculin": "H",
            "Dim: Sexe (3): Membre ID: [3]: Sexe féminin": "F"
        },
        "SDR_dtypes": {
            "CODE_GÉO_ALT": "string",
            "NIVEAU_GÉO": "category",
            "Membre ID: Profil des aires de diffusion (57)": "int16",
            "DIM: Profil des aires de diffusion (57)": "category"
        },
        "SR": {
            "CODE_GÉO": "GEO_CODE",
            "NIVEAU_GÉO": "TYPE_GEO",
            "Membre ID: Profil des secteurs de recensement (57)": "MODALITE_ID",
            "DIM: Profil des secteurs de recensement (57)": "MODALITE_NOM",
            "Dim: Sexe (3): Membre ID: [1]: Total - Sexe": "TOTAL",
            "Dim: Sexe (3): Membre ID: [2]: Sexe masculin": "H",
            "Dim: Sexe (3): Membre ID: [3]: Sexe féminin": "F"
        },
        "SR_dtypes": {
            "CODE_GÉO": "string",
            "NIVEAU_GÉO": "category",
            "Membre ID: Profil des secteurs de recensement (57)": "int16",
            "DIM: Profil des secteurs de recensement (57)": "category"
        }
    },
    "2011": {
        "SDR": {
            "Geo_Code": "GEO_CODE",
            "Caractéristiques": "MODALITE_NOM",
            "Total": "TOTAL",
            "Sexe_masculin": "H",
            "Sexe_féminin": "F"
        },
        "SDR_dtypes": {
            "Geo_Code": "string",
            "NIVEAU_GÉO": "category",
            "Caractéristiques": "category"
        },
        "SR": {
            "Geo_Code": "GEO_CODE",
            "Caractéristique": "MODALITE_NOM",
            "Total": "TOTAL",
            "Sexe_masculin": "H",
            "Sexe_féminin": "F"
        },
        "SR_dtypes": {
            "Geo_Code": "string",
            "NIVEAU_GÉO": "category",
            "Caractéristiques": "category"
        }
    },
    "2006": {
        "SDR": {
            "Geo_Code": "GEO_CODE",
            "Caractéristiques": "MODALITE_NOM",
            "Total": "TOTAL",
            "Sexe_masculin": "H",
            "Sexe_féminin": "F"
        },
        "SDR_dtypes": {
            "Geo_Code": "string",
            "NIVEAU_GÉO": "category",
            "Caractéristiques": "category"
        },
        "SR": {
            "Geo_Code": "GEO_CODE",
            "Caractéristique": "MODALITE_NOM",
            "Total": "TOTAL",
            "Sexe_masculin": "H",
            "Sexe_féminin": "F"
        },
        "SR_dtypes": {
            "Geo_Code": "string",
            "NIVEAU_GÉO": "category",
            "Caractéristiques": "category"
        }
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# CLASSES


class Profil:
    def __init__(self, annee: str, sr: bool, sdr: bool, varlist_rename: dict):
        self.annee = annee
        self.varlist_rename = varlist_rename[annee]
        self.df_dtypes_sr = varlist_rename[annee]["SR_dtypes"]
        self.df_dtypes_sdr = varlist_rename[annee]["SDR_dtypes"]

        if sr:
            self.clean_sr(f'Fichier_source_profil_{self.annee}_SR.csv')
        if sdr:
            self.clean_sdr(f'Fichier_source_profil_{self.annee}_SDR.csv')

    def clean_sr(self, filename):
        """
        fonction qui extrait les SR du Québec d'un .csv
        :param filename: chemin d'accès du csv d'input
        :return: NA. Exporte un csv clean et renommé
        """
        df = (
            pd.read_csv(
                f'{params["working_directory"]}{params["input_path"]}{filename}',
                sep=",",
                dtype=self.df_dtypes_sr
            )
            .rename(mapper=self.varlist_rename["SR"], axis=1)
        )
        df = df[df["GEO_CODE"].str.startswith(('408', '421', '442', '433', '462', '505'))]
        df = df[df["GEO_CODE"].map(len) == 10]

        # Enlever les SR d'Ottawa
        # convertir le code_geo en nombre
        df['temp_ottawa'] = (
            df["GEO_CODE"]
            .astype('float')
        )
        df = (
            df[~((df['temp_ottawa'] >= 5051000) & (df['temp_ottawa'] < 5060000))]
            .drop('temp_ottawa', axis=1)
        )

        if self.annee == "2011":
            df["MODALITE_ID"] = np.tile(list(range(1, 473)), len(df) // 472 + 1)[:len(df)].astype("int32")

        df.to_csv(
            f'{params["working_directory"]}{params["output_path"]}profil_{self.annee}_sr_qc.csv',
            sep=";",
            encoding='utf-8-sig',
            index=False
        )

    def clean_sdr(self, filename):
        """
        importe, nettoie puis exporte le fichier du profil du recensement par SDR du Québec
        :param self:
        :param filename:
        :return:
        """
        df = (
            pd
            .read_csv(
                f'{params["working_directory"]}{params["input_path"]}{filename}',
                sep=",",
                dtype=self.df_dtypes_sdr
            )
            .rename(mapper=self.varlist_rename["SDR"], axis=1)
        )
        if self.annee == "2016":
            df = df[df["GEO_CODE"].map(len) == 7]
            df = df[df["GEO_CODE"].str.startswith('24')]

        if self.annee == "2011":
            df["MODALITE_ID"] = np.tile(list(range(1, 473)), len(df) // 472 + 1)[:len(df)].astype("int32")

        df.to_csv(
            f'{params["working_directory"]}{params["output_path"]}profil_{self.annee}_sdr_qc.csv',
            sep=";",
            encoding='utf-8-sig',
            index=False
        )

# ----------------------------------------------------------------------------------------------------------------------
# MAIN


if params["annee"] == "2021":
    profil_2021 = Profil(
        annee="2021",
        sr=params["SR"],
        sdr=params["SDR"],
        varlist_rename=dict_varlist_rename_and_type,
    )

if params["annee"] == "2016":
    profil_2016 = Profil(
        annee="2016",
        sr=params["SR"],
        sdr=params["SDR"],
        varlist_rename=dict_varlist_rename_and_type,
    )

if params["annee"] == "2011":
    profil_2011 = Profil(
        annee="2011",
        sr=params["SR"],
        sdr=params["SDR"],
        varlist_rename=dict_varlist_rename_and_type,
    )
pass
