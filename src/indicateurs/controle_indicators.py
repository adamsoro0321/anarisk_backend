"""
Indicateurs de risque liés aux contrôles fiscaux
Reproduction des fonctions de contrôle du script R
Version optimisée avec opérations vectorisées

Indicateurs couverts:
- IND_15_A: Absence de Vérification Générale (VG) depuis plus de 3 ans
- IND_15_B: Absence de Vérification Ponctuelle (VP) depuis plus de 3 ans
- IND_16: Absence prolongée de contrôle (VG > 4 ans et pas d'avis)
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional


class ControleIndicators:
    """
    Classe pour calculer les indicateurs de risque liés aux contrôles fiscaux.
    Version optimisée avec assignation directe dans risk_df.
    """

    # Configuration des seuils (en années)
    SEUIL_ANNEES_IND_15 = 3  # 3 ans pour l'indicateur 15
    SEUIL_ANNEES_IND_16 = 4  # 4 ans pour l'indicateur 16
    
    # Dates fixes par défaut (comme dans le script R original)
    _DATE_FIXE_IND_15 = pd.Timestamp("2022-12-31")
    _DATE_FIXE_IND_16 = pd.Timestamp("2021-12-31")

    @staticmethod
    def _get_date_seuil(annees: int, date_reference: Optional[datetime] = None) -> pd.Timestamp:
        """Calcule la date seuil dynamique (date_reference - annees)."""
        if date_reference is None:
            date_reference = datetime.now()
        return pd.Timestamp(date_reference - relativedelta(years=annees))

    @staticmethod
    def _to_datetime_values(series: pd.Series) -> np.ndarray:
        """Convertit une Series en array datetime64 sans copier le DataFrame entier."""
        return pd.to_datetime(series, errors='coerce').values

    @classmethod
    def calculate_indicator_15(
        cls,
        merged_data: pd.DataFrame, 
        risk_df: pd.DataFrame,
        use_dynamic_date: bool = True,
        date_reference: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        INDICATEURS 15A et 15B: Absence de contrôles depuis plus de 3 ans
        
        Logique R:
        - 15A: DATE_DERNIERE_VG < seuil ET DATE_DERNIERE_AVIS < seuil → "rouge"
        - 15B: DATE_DERNIERE_VP < seuil ET DATE_DERNIERE_AVIS < seuil → "rouge"
        
        Version optimisée avec assignation directe.
        """
        # Date seuil
        date_seuil = (
            cls._get_date_seuil(cls.SEUIL_ANNEES_IND_15, date_reference)
            if use_dynamic_date else cls._DATE_FIXE_IND_15
        )
        date_seuil_ns = np.datetime64(date_seuil)

        # Convertir les colonnes de dates en arrays numpy (sans copier tout le DataFrame)
        date_vg = cls._to_datetime_values(merged_data["DATE_DERNIERE_VG"])
        date_vp = cls._to_datetime_values(merged_data["DATE_DERNIERE_VP"])
        date_avis = cls._to_datetime_values(merged_data["DATE_DERNIERE_AVIS"])

        # Initialiser les colonnes
        risk_df["RISQUE_IND_15_A"] = "vert"
        risk_df["RISQUE_IND_15_B"] = "vert"

        # INDICATEUR 15A: VG < seuil ET AVIS < seuil
        mask_15A = (date_vg < date_seuil_ns) & (date_avis < date_seuil_ns)
        risk_df.loc[mask_15A, "RISQUE_IND_15_A"] = "rouge"

        # INDICATEUR 15B: VP < seuil ET AVIS < seuil
        mask_15B = (date_vp < date_seuil_ns) & (date_avis < date_seuil_ns)
        risk_df.loc[mask_15B, "RISQUE_IND_15_B"] = "rouge"

        return risk_df

    @classmethod
    def calculate_indicator_16(
        cls,
        merged_data: pd.DataFrame, 
        risk_df: pd.DataFrame,
        use_dynamic_date: bool = True,
        date_reference: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        INDICATEUR 16: Absence prolongée de contrôle
        
        Logique: DATE_DERNIERE_VG < seuil (4 ans) ET DATE_DERNIERE_AVIS est NA
        
        Version optimisée avec assignation directe.
        """
        # Date seuil (4 ans)
        date_seuil = (
            cls._get_date_seuil(cls.SEUIL_ANNEES_IND_16, date_reference)
            if use_dynamic_date else cls._DATE_FIXE_IND_16
        )
        date_seuil_ns = np.datetime64(date_seuil)

        # Convertir les colonnes de dates
        date_vg = cls._to_datetime_values(merged_data["DATE_DERNIERE_VG"])
        date_avis = cls._to_datetime_values(merged_data["DATE_DERNIERE_AVIS"])

        # Initialiser
        risk_df["RISQUE_IND_16"] = "vert"

        # Condition: VG < seuil ET AVIS est NA (NaT en datetime)
        mask_16 = (date_vg < date_seuil_ns) & pd.isna(date_avis)
        risk_df.loc[mask_16, "RISQUE_IND_16"] = "rouge"

        return risk_df

    @classmethod
    def get_liste_non_eligibles(cls, risk_df: pd.DataFrame) -> np.ndarray:
        """
        Extrait les IFU des entreprises à risque (équivalent listeNonEl en R).
        Retourne un array numpy pour meilleure performance.
        """
        mask_rouge = (
            (risk_df["RISQUE_IND_15_A"] == "rouge") | 
            (risk_df["RISQUE_IND_15_B"] == "rouge")
        )
        return risk_df.loc[mask_rouge, "NUM_IFU"].unique()

    @classmethod
    def calculate_all_indicators(
        cls,
        merged_data: pd.DataFrame,
        risk_df: pd.DataFrame,
        use_dynamic_date: bool = True,
        date_reference: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Calcule tous les indicateurs de contrôle en une seule fois."""
        risk_df = cls.calculate_indicator_15(merged_data, risk_df, use_dynamic_date, date_reference)
        risk_df = cls.calculate_indicator_16(merged_data, risk_df, use_dynamic_date, date_reference)
        risk_df = cls.calculate_indicator_30(merged_data, risk_df)
        return risk_df

    @staticmethod
    def get_indicator_summary(risk_df: pd.DataFrame) -> dict:
        """Génère un résumé statistique des indicateurs de contrôle."""
        summary = {}
        indicators = ["RISQUE_IND_15_A", "RISQUE_IND_15_B", "RISQUE_IND_16", "RISQUE_IND_30"]
        total = len(risk_df)
        
        for indicator in indicators:
            if indicator in risk_df.columns:
                counts = risk_df[indicator].value_counts()
                rouge_count = counts.get("rouge", 0)
                summary[indicator] = {
                    "rouge": rouge_count,
                    "vert": counts.get("vert", 0),
                    "non_disponible": counts.get("Non disponible", 0),
                    "total": total,
                    "taux_risque": round(rouge_count / total * 100, 2) if total > 0 else 0
                }
        
        return summary

    @staticmethod
    def calculate_indicator_30(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 30: Cohérence résultat financier vs provisions (OPTIMISÉ)
        
        Logique R:
            1. VAR_XF = variation de XF_RESULT_FIN_31_12_N_Net par IFU (diff année N vs N-1)
            2. SOM_PF = TtlProviRisqCharg_AnneeN + DN_ProviRisqChargCourTerm_Exer31_12_N_Net
            3. VAR_PF = variation de SOM_PF par IFU
            4. RATIO_30_1 = VAR_XF / XB_CA_31_12_N_Net
            5. RATIO_30_2 = VAR_PF / XB_CA_31_12_N_Net
            6. Si RATIO_30_1 < 0 ET RATIO_30_2 > 0 → rouge, sinon vert
            
        Interprétation: Détecte les situations suspectes où le résultat financier 
        diminue (ratio négatif) alors que les provisions augmentent (ratio positif).
        """
        # Initialiser la colonne
        if "RISQUE_IND_30" not in risk_df.columns:
            risk_df["RISQUE_IND_30"] = "Non disponible"

        # Chercher les colonnes avec différentes casses
        def find_col(df, candidates):
            for c in candidates:
                if c in df.columns:
                    return c
                if c.upper() in df.columns:
                    return c.upper()
                if c.lower() in df.columns:
                    return c.lower()
            return None

        xf_col = find_col(merged_data, ["XF_RESULT_FIN_31_12_N_Net", "XF_RESULT_FIN_31_12_N_NET"])
        prov1_col = find_col(merged_data, ["TtlProviRisqCharg_AnneeN", "TTLPROVIRISQCHARG_ANNEEN"])
        prov2_col = find_col(merged_data, ["DN_ProviRisqChargCourTerm_Exer31_12_N_Net", "DN_PROVIRISQCHARGCOURTERM_EXER31_12_N_NET"])
        ca_col = find_col(merged_data, ["XB_CA_31_12_N_Net", "XB_CA_31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if xf_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_XF"] = pd.to_numeric(df[xf_col], errors="coerce").fillna(0)
        df["_CA"] = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)
        
        # Somme des provisions
        prov1 = pd.to_numeric(df[prov1_col], errors="coerce").fillna(0) if prov1_col else 0
        prov2 = pd.to_numeric(df[prov2_col], errors="coerce").fillna(0) if prov2_col else 0
        df["_SOM_PF"] = prov1 + prov2

        # Trier par IFU et année pour calculer les variations correctement
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (équivalent à ave(..., FUN = function(x) c(0, diff(x))))
        df["VAR_XF"] = df.groupby(ifu_col)["_XF"].diff().fillna(0)
        df["VAR_PF"] = df.groupby(ifu_col)["_SOM_PF"].diff().fillna(0)

        # Calcul des ratios (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_30_1"] = np.where(
                (df["_CA"] != 0) & df["_CA"].notna() & df["VAR_XF"].notna(),
                df["VAR_XF"] / df["_CA"],
                0
            )
            df["RATIO_30_2"] = np.where(
                (df["_CA"] != 0) & df["_CA"].notna() & df["VAR_PF"].notna(),
                df["VAR_PF"] / df["_CA"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_30_1 < 0 ET RATIO_30_2 > 0
        rouge_mask = (df["RATIO_30_1"] < 0) & (df["RATIO_30_2"] > 0)
        
        df["_RISQUE_30"] = np.where(rouge_mask, "rouge", "vert")

        # Joindre les résultats à risk_df
        # Créer un mapping IFU+ANNEE -> risque (garder uniquement les combinaisons uniques)
        result_map = df[[ifu_col, annee_col, "_RISQUE_30"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger avec risk_df
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map.rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left"
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            # Supprimer les duplicats créés par le merge
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer le résultat
        risk_df["RISQUE_IND_30"] = risk_df["_RISQUE_30"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_30"], errors="ignore")

        return risk_df
