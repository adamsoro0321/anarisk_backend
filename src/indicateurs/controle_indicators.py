"""
Indicateurs de risque liés aux contrôles fiscaux
Reproduction des fonctions de contrôle du script R
"""

import pandas as pd
from datetime import datetime


class ControleIndicators:
    """Classe pour calculer les indicateurs de risque contrôle"""

    @staticmethod
    def calculate_indicator_15(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEURS 15A et 15B: Absence de contrôles
        15A: DATE_DERNIERE_VG et DATE_DERNIERE_AVIS < 2021-12-31
        15B: DATE_DERNIERE_VP et DATE_DERNIERE_AVIS < 2021-12-31
        """

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_15_A" not in risk_df.columns:
            risk_df["RISQUE_IND_15_A"] = "Non disponible"
        if "RISQUE_IND_15_B" not in risk_df.columns:
            risk_df["RISQUE_IND_15_B"] = "Non disponible"

        date_seuil = datetime.strptime("2021-12-31", "%Y-%m-%d").date()

        # Indicateur 15A: Vérification générale
        for i in range(len(merged_data)):
            date_vg = merged_data.iloc[i].get("DATE_DERNIERE_VG")
            date_avis = merged_data.iloc[i].get("DATE_DERNIERE_AVIS")
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            # Cibler avec NUM_IFU et ANNEE
            mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)

            try:
                if isinstance(date_vg, str):
                    date_vg = pd.to_datetime(date_vg).date()
                if isinstance(date_avis, str):
                    date_avis = pd.to_datetime(date_avis).date()

                if (
                    not pd.isna(date_vg)
                    and not pd.isna(date_avis)
                    and date_vg < date_seuil
                    and date_avis < date_seuil
                ):
                    risk_df.loc[mask, "RISQUE_IND_15_A"] = "rouge"
                else:
                    risk_df.loc[mask, "RISQUE_IND_15_A"] = "vert"
            except Exception:
                risk_df.loc[mask, "RISQUE_IND_15_A"] = "vert"

        # Indicateur 15B: Vérification ponctuelle
        for i in range(len(merged_data)):
            date_vp = merged_data.iloc[i].get("DATE_DERNIERE_VP")
            date_avis = merged_data.iloc[i].get("DATE_DERNIERE_AVIS")
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            # Cibler avec NUM_IFU et ANNEE
            mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)

            try:
                if isinstance(date_vp, str):
                    date_vp = pd.to_datetime(date_vp).date()
                if isinstance(date_avis, str):
                    date_avis = pd.to_datetime(date_avis).date()

                if (
                    not pd.isna(date_vp)
                    and not pd.isna(date_avis)
                    and date_vp < date_seuil
                    and date_avis < date_seuil
                ):
                    risk_df.loc[mask, "RISQUE_IND_15_B"] = "rouge"
                else:
                    risk_df.loc[mask, "RISQUE_IND_15_B"] = "vert"
            except Exception:
                risk_df.loc[mask, "RISQUE_IND_15_B"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_16(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 16: Absence prolongée de contrôle
        Logique: DATE_DERNIERE_VG < 2020-12-31 et DATE_DERNIERE_AVIS vide
        """

        # Créer la colonne de résultats si elle n'existe pas
        if "RISQUE_IND_16" not in risk_df.columns:
            risk_df["RISQUE_IND_16"] = "Non disponible"

        date_seuil = datetime.strptime("2020-12-31", "%Y-%m-%d").date()

        # Application du calcul
        for i in range(len(merged_data)):
            date_vg = merged_data.iloc[i].get("DATE_DERNIERE_VG")
            date_avis = merged_data.iloc[i].get("DATE_DERNIERE_AVIS")
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            # Cibler avec NUM_IFU et ANNEE
            mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)

            try:
                if isinstance(date_vg, str):
                    date_vg = pd.to_datetime(date_vg).date()

                if not pd.isna(date_vg) and date_vg < date_seuil and pd.isna(date_avis):
                    risk_df.loc[mask, "RISQUE_IND_16"] = "rouge"
                else:
                    risk_df.loc[mask, "RISQUE_IND_16"] = "vert"
            except Exception:
                risk_df.loc[mask, "RISQUE_IND_16"] = "vert"

        return risk_df
