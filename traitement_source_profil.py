# Outil pour traiter les fichiers source de profil de recensement par sr et sdr.
import pandas as pd

# ----------------------------------------------------------------------------------------------------------------------
# PARAMS


params = {
    "annee": "2021",
    "working_directory": f'C:/Projets_Python/Outils_Demo/',
    "input_path": f'inputs/',
    "output_path": f'outputs/',
    "SR": True,
    "SDR": True
}

# code inscrit dans le fichier de profil qui permet d'identifier l'année et le type de découpage géographique
code_profil = {
    "2021SDR": "X2021020",
    "2021SR": "X2021007",
    "2016SDR": "X2016004",
    "2016SR": "X2016003"
}

dict_varlist_rename = {
    "2021": {
        "SDR": {
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
        }
    },
    "2016": {
        "SDR": {
            "CODE_GÉO": "GEO_CODE",
            "NIVEAU_GÉO": "TYPE_GEO",
            "Membre ID: Profil des aires de diffusion (57)": "MODALITE_ID",
            "DIM: Profil des aires de diffusion (57)": "MODALITE_NOM",
            "Dim: Sexe (3): Membre ID: [1]: Total - Sexe": "TOTAL",
            "Dim: Sexe (3): Membre ID: [2]: Sexe masculin": "H",
            "Dim: Sexe (3): Membre ID: [3]: Sexe féminin": "F"
        },
        "SR": {
            "CODE_GÉO": "GEO_CODE",
            "NIVEAU_GÉO": "TYPE_GEO",
            "Membre ID: Profil des secteurs de recensement (57)": "MODALITE_ID",
            "DIM: Profil des secteurs de recensement (57)": "MODALITE_NOM",
            "Dim: Sexe (3): Membre ID: [1]: Total - Sexe": "TOTAL",
            "Dim: Sexe (3): Membre ID: [2]: Sexe masculin": "H",
            "Dim: Sexe (3): Membre ID: [3]: Sexe féminin": "F"
        }
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# CLASSES


class Profil:
    def __init__(self, annee: str, sr: bool, sdr: bool, varlist_rename: dict):
        self.annee = annee
        self.varlist_rename = varlist_rename[annee]
        self.id_modalite = id_modalites

        if sr:
            self.clean_sr(f'98-401-{code_profil[(self.annee+"SR")]}_Francais_CSV_data.csv')
        if sdr:
            self.clean_sdr(f'98-401-{code_profil[(self.annee+"SDR")]}_Francais_CSV_data.csv')

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
                dtype={
                    "NOM_GÉO": "string",
                    "CODE_GÉO": "string"
                }
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
        df.to_csv(
            f'{params["working_directory"]}{params["output_path"]}profil_{self.annee}_sr_qc.csv',
            sep=";",
            encoding='utf-8-sig',
            index=False
        )

    def clean_sdr(self, filename):
        """
        importe, nettoie puis exporte le fichier du profil du recsenement par SDR du Québec
        :param self:
        :param filename:
        :return:
        """
        df = (
            pd
            .read_csv(
                f'{params["working_directory"]}{params["input_path"]}{filename}',
                sep=",",
                dtype={
                    "CODE_GÉO": "string"
                }
            )
            .rename(mapper=self.varlist_rename["SDR"], axis=1)
        )
        if self.annee == "2016":
            df = df[df["GEO_CODE"].map(len) == 7]
            df = df[df["GEO_CODE"].str.startswith('24')]

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
        varlist_rename=dict_varlist_rename,
    )

if params["annee"] == "2016":
    profil_2016 = Profil(
        annee="2016",
        sr=params["SR"],
        sdr=params["SDR"],
        varlist_rename=dict_varlist_rename,
    )
pass
