"""
Indicateurs de risque liés aux importations et exportations
Reproduction des fonctions d'import/export du script R
"""

import pandas as pd
from datetime import datetime
from typing import List


class ImportExportIndicators:
    """Classe pour calculer les indicateurs de risque import/export"""

    @staticmethod
    def calculate_indicator_3(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 3: Nouveau contribuable importateur important
        Logique: Entreprise créée il y a moins de 12 mois avec importations >= 100M
        """

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_3" not in risk_df.columns:
            risk_df["RISQUE_IND_3"] = "Non disponible"
        if "AGE_MOIS_IND_3" not in risk_df.columns:
            risk_df["AGE_MOIS_IND_3"] = 0

        def tva_ind3(date_creation, importation):
            """Calcul pour l'indicateur 3"""
            if pd.isna(date_creation) or pd.isna(importation):
                return ["Non disponible", 0]

            # Conversion de la date si nécessaire
            if isinstance(date_creation, str):
                try:
                    date_creation = pd.to_datetime(date_creation)
                except Exception:
                    return ["Non disponible", 0]

            # Calcul de l'âge en mois
            date_actuelle = datetime.now()
            duree_mois = (
                date_actuelle - date_creation
            ).days / 30.44  # Moyenne de jours par mois

            if duree_mois <= 12 and importation >= 100000000:
                risque = "rouge"
            else:
                risque = "vert"

            return [risque, duree_mois]

        # Application du calcul
        for i in range(len(merged_data)):
            date_immat = merged_data.iloc[i].get("DATE_IMMAT")
            import_caf = merged_data.iloc[i].get("IMPORT_CAF", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            result = tva_ind3(date_immat, import_caf)

            # Cibler avec NUM_IFU et ANNEE
            mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
            risk_df.loc[mask, "RISQUE_IND_3"] = result[0]
            risk_df.loc[mask, "AGE_MOIS_IND_3"] = result[1]

        return risk_df

    @staticmethod
    def calculate_indicator_4(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 4: Importateur dépassant les seuils de régime
        Logique: CME > 15M ou RSI > 50M
        """

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_4" not in risk_df.columns:
            risk_df["RISQUE_IND_4"] = "Non disponible"
        if "GAP_IND_4" not in risk_df.columns:
            risk_df["GAP_IND_4"] = 0
        if "SCORE_IND_4" not in risk_df.columns:
            risk_df["SCORE_IND_4"] = 0

        def ind4(criticite, numerateur, regime, seuil, coeff, x1, x2, x3, x4):
            if not pd.isna(numerateur) and not pd.isna(regime):
                if regime == "CME":
                    denominateur = 15000000
                elif regime == "RSI":
                    denominateur = 50000000
                else:
                    indicateur = 0
                    denominateur = 0

                if denominateur > 0:
                    indicateur = numerateur / denominateur
                else:
                    indicateur = 0
            else:
                indicateur = 0
                denominateur = 0

            if indicateur < seuil:
                groupe = "vert"
                ecart = 0
                score = 0
                return [ecart, groupe, score]
            else:
                ecart = abs(numerateur - denominateur)
                ecart = ecart * coeff

                if ecart < x1:
                    impact = 1
                elif ecart < x2:
                    impact = 2
                elif ecart < x3:
                    impact = 3
                elif ecart < x4:
                    impact = 4
                else:
                    impact = 5

                score = criticite * impact

                # Détermination du groupe
                if score in [1, 2, 3, 4]:
                    groupe = "vert"
                elif score == 5:
                    groupe = "vert" if criticite == 1 else "jaune"
                elif score == 6:
                    groupe = "vert" if criticite == 2 else "jaune"
                elif score in [8, 9]:
                    groupe = "jaune"
                elif score == 10:
                    groupe = "jaune" if criticite == 2 else "rouge"
                elif score in [12, 16]:
                    groupe = "orange"
                elif score == 15:
                    groupe = "orange" if criticite == 3 else "rouge"
                elif score in [20, 25]:
                    groupe = "rouge"
                else:
                    groupe = "vert"

                return [ecart, groupe, score]

        # Application du calcul
        for i in range(len(merged_data)):
            import_caf = merged_data.iloc[i].get("IMPORT_CAF", 0)
            code_reg_fisc = merged_data.iloc[i].get("CODE_REG_FISC")
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(import_caf) and not pd.isna(code_reg_fisc):
                result = ind4(
                    5,
                    import_caf,
                    code_reg_fisc,
                    1,
                    0.8,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )
                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_4"] = result[1]
                risk_df.loc[mask, "GAP_IND_4"] = result[0]
                risk_df.loc[mask, "SCORE_IND_4"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_4"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_4"] = 0
                risk_df.loc[mask, "SCORE_IND_4"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_5(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 5: Cohérence des exportations déclarées TVA vs DGD
        Logique: MNT_EXPORTATION_DECLARE / EXPORT_CAF >= 1
        """

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_5" not in risk_df.columns:
            risk_df["RISQUE_IND_5"] = "Non disponible"
        if "GAP_IND_5" not in risk_df.columns:
            risk_df["GAP_IND_5"] = 0
        if "SCORE_IND_5" not in risk_df.columns:
            risk_df["SCORE_IND_5"] = 0

        def ind5(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4):
            if (
                denominateur != 0
                and not pd.isna(denominateur)
                and not pd.isna(numerateur)
            ):
                indicateur = numerateur / denominateur
            else:
                indicateur = 0

            if indicateur >= seuil:
                groupe = "vert"
                ecart = 0
                score = 0
                return [ecart, groupe, score]
            else:
                ecart = abs(denominateur - numerateur)
                ecart = ecart * coeff

                if ecart < x1:
                    impact = 1
                elif ecart < x2:
                    impact = 2
                elif ecart < x3:
                    impact = 3
                elif ecart < x4:
                    impact = 4
                else:
                    impact = 5

                score = criticite * impact

                # Détermination du groupe
                if score in [1, 2, 3, 4]:
                    groupe = "vert"
                elif score == 5:
                    groupe = "vert" if criticite == 1 else "jaune"
                elif score == 6:
                    groupe = "vert" if criticite == 2 else "jaune"
                elif score in [8, 9]:
                    groupe = "jaune"
                elif score == 10:
                    groupe = "jaune" if criticite == 2 else "rouge"
                elif score in [12, 16]:
                    groupe = "orange"
                elif score == 15:
                    groupe = "orange" if criticite == 3 else "rouge"
                elif score in [20, 25]:
                    groupe = "rouge"
                else:
                    groupe = "vert"

                return [ecart, groupe, score]

        # Application du calcul
        for i in range(len(merged_data)):
            mnt_exportation_declare = merged_data.iloc[i].get(
                "OP_NTAXBLE_EXPORTATIONS", 0
            )
            export_caf = merged_data.iloc[i].get("EXPORT_CAF", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(mnt_exportation_declare) and not pd.isna(export_caf):
                result = ind5(
                    3,
                    mnt_exportation_declare,
                    export_caf,
                    1,
                    0.5,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )
                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_5"] = result[1]
                risk_df.loc[mask, "GAP_IND_5"] = result[0]
                risk_df.loc[mask, "SCORE_IND_5"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_5"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_5"] = 0
                risk_df.loc[mask, "SCORE_IND_5"] = 0

        return risk_df
