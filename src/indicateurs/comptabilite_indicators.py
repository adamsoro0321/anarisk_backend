"""
Indicateurs de risque liés à la comptabilité
Reproduction des fonctions comptables du script R
Version optimisée avec opérations vectorisées
"""

import pandas as pd
import numpy as np
from typing import Tuple


# Colonnes pour le calcul de l'acquisition d'immobilisation (IND_6)
ACQUISITION_IMMOBILISATION_COLUMNS = [
    "FraisDevProsp_AcqApCreat",
    "FraisDevProsp_VirepostPost",
    "BrevLicLogDroiSim_AcqApCreat",
    "BrevLicLogDroiSim_VirepostPost",
    "FdsCommDroiB_AcqApCreat",
    "FdsCommDroiB_VirepostPost",
    "AutrImmbIncorp_AcqApCreat",
    "AutrImmbIncorp_VirepostPost",
    "TerrHorsImmePlac_AcqApCreat",
    "TerrHorsImmePlac_VirepostPost",
    "TerrImmePlac_AcqApCreat",
    "TerrImmePlac_VirepostPost",
    "BatHorsImmePlac_AcqApCreat",
    "BatHorsImmePlac_VirepostPost",
    "BatImmePlac_AcqApCreat",
    "BatImmePlac_VirepostPost",
    "amenagAgenInst_AcqApCreat",
    "amenagAgenInst_VirepostPost",
    "MatMobActBiol_AcqApCreat",
    "MatMobActBiol_VirepostPost",
    "MatTransp_AcqApCreat",
    "MatTransp_VirepostPost",
]


def get_groupe_from_score(score: int, criticite: int) -> str:
    """
    Détermine le groupe de risque selon le score et la criticité.
    Fonction utilitaire réutilisable pour tous les indicateurs.
    """
    if score in [1, 2, 3, 4]:
        return "vert"
    elif score == 5:
        return "vert" if criticite == 1 else "jaune"
    elif score == 6:
        return "vert" if criticite == 2 else "jaune"
    elif score in [8, 9]:
        return "jaune"
    elif score == 10:
        return "jaune" if criticite == 2 else "rouge"
    elif score in [12, 16]:
        return "orange"
    elif score == 15:
        return "orange" if criticite == 3 else "rouge"
    elif score in [20, 25]:
        return "rouge"
    else:
        return "vert"


def get_groupe_vectorized(score: np.ndarray, criticite: int) -> np.ndarray:
    """
    Version vectorisée de get_groupe_from_score.
    Retourne un array de groupes basé sur les scores.
    """
    # Conditions basées sur la criticité
    if criticite == 1:
        return np.select(
            [
                np.isin(score, [1, 2, 3, 4, 5]),
                score == 6,
                np.isin(score, [8, 9]),
                score == 10,
                np.isin(score, [12, 16]),
                score == 15,
                np.isin(score, [20, 25]),
            ],
            ["vert", "jaune", "jaune", "rouge", "orange", "rouge", "rouge"],
            default="vert"
        )
    elif criticite == 2:
        return np.select(
            [
                np.isin(score, [1, 2, 3, 4]),
                score == 5,
                score == 6,
                np.isin(score, [8, 9]),
                score == 10,
                np.isin(score, [12, 16]),
                score == 15,
                np.isin(score, [20, 25]),
            ],
            ["vert", "jaune", "vert", "jaune", "jaune", "orange", "rouge", "rouge"],
            default="vert"
        )
    elif criticite == 3:
        return np.select(
            [
                np.isin(score, [1, 2, 3, 4]),
                score == 5,
                score == 6,
                np.isin(score, [8, 9]),
                score == 10,
                np.isin(score, [12, 16]),
                score == 15,
                np.isin(score, [20, 25]),
            ],
            ["vert", "jaune", "jaune", "jaune", "rouge", "orange", "orange", "rouge"],
            default="vert"
        )
    else:  # criticite >= 4 (4 ou 5)
        return np.select(
            [
                np.isin(score, [1, 2, 3, 4]),
                score == 5,
                score == 6,
                np.isin(score, [8, 9]),
                score == 10,
                np.isin(score, [12, 16]),
                score == 15,
                np.isin(score, [20, 25]),
            ],
            ["vert", "jaune", "jaune", "jaune", "rouge", "orange", "rouge", "rouge"],
            default="vert"
        )


def calculate_impact_vectorized(ecart: np.ndarray, x1: float, x2: float, x3: float, x4: float) -> np.ndarray:
    """
    Calcule l'impact selon l'écart et les seuils (version vectorisée).
    """
    return np.select(
        [ecart < x1, ecart < x2, ecart < x3, ecart < x4],
        [1, 2, 3, 4],
        default=5
    )


class ComptabiliteIndicators:
    """Classe pour calculer les indicateurs de risque comptabilité (version optimisée)"""

    @staticmethod
    def calculate_indicator_9(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 9: Marge commerciale négative (OPTIMISÉ)
        Logique R: TVA_IND9
        - Si marge commerciale < 0 → rouge
        - Si marge commerciale >= 0 → vert
        - Si marge commerciale NA → Non disponible

        Complexité: O(n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_9" not in risk_df.columns:
            risk_df["RISQUE_IND_9"] = "Non disponible"

        # Vérifier que la colonne source existe
        if "XA_MargCommerc_31_12_N_Net" not in merged_data.columns:
            return risk_df
        
        # Merge pour jointure efficace O(n log n) au lieu de O(n²)
        df = risk_df.merge(
            merged_data[["NUM_IFU", "ANNEE", "XA_MargCommerc_31_12_N_Net"]].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left"
        )
        
        # Calcul vectorisé avec np.select O(n)
        marge = df["XA_MargCommerc_31_12_N_Net"]
        conditions = [
            marge.notna() & (marge < 0),
            marge.notna() & (marge >= 0),
        ]
        choices = ["rouge", "vert"]
        
        risk_df["RISQUE_IND_9"] = np.select(conditions, choices, default="Non disponible")
        
        return risk_df

    @staticmethod
    def calculate_indicator_6(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 6: Acquisition d'immobilisation vs Chiffre d'Affaires (OPTIMISÉ)
        
        Logique R: IND_6
        - Seuil dynamique: seuil = 0.4 (40% du CA)
        - Si indicateur < seuil → vert
        - Sinon calcul de l'écart et score
        
        Paramètres R: criticite=5, coeff=0.9, x1=500K, x2=5M, x3=20M, x4=100M
        
        Complexité: O(n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_6" not in risk_df.columns:
            risk_df["RISQUE_IND_6"] = "Non disponible"
        if "GAP_IND_6" not in risk_df.columns:
            risk_df["GAP_IND_6"] = 0.0
        if "SCORE_IND_6" not in risk_df.columns:
            risk_df["SCORE_IND_6"] = 0

        # Sélectionner les colonnes nécessaires (uniquement celles qui existent)
        cols_needed = ["NUM_IFU", "ANNEE", "XB_CA_31_12_N_Net"]
        existing_immob_cols = [col for col in ACQUISITION_IMMOBILISATION_COLUMNS if col in merged_data.columns]
        cols_needed.extend(existing_immob_cols)
        existing_cols = [col for col in cols_needed if col in merged_data.columns]
        
        # Merge efficace O(n log n)
        df = risk_df.merge(
            merged_data[existing_cols].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left"
        )
        
        # Calcul vectorisé de l'acquisition d'immobilisation
        if existing_immob_cols:
            acquisition = df[existing_immob_cols].fillna(0).sum(axis=1)
        else:
            acquisition = pd.Series([0] * len(df))
        
        # Paramètres
        criticite = 5
        seuil = 0.4
        coeff = 0.9
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000
        
        # Calcul vectorisé
        ca_net = df.get("XB_CA_31_12_N_Net", pd.Series([0] * len(df))).fillna(0)
        
        # Indicateur: acquisition / CA
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(ca_net != 0, acquisition / ca_net, 0)
        
        # Condition vert: indicateur < seuil
        is_vert = indicateur < seuil
        
        # Calcul écart et score pour tous (sera 0 pour les verts)
        ecart = np.abs((seuil * ca_net) - acquisition) * coeff
        
        # Impact vectorisé
        impact = calculate_impact_vectorized(ecart, x1, x2, x3, x4)
        score = criticite * impact
        
        # Groupe vectorisé
        groupe = get_groupe_vectorized(score, criticite)
        
        # Appliquer vert pour ceux qui sont en dessous du seuil
        groupe = np.where(is_vert, "vert", groupe)
        
        # Gestion des NA (pas de CA)
        has_data = ca_net.notna() & (ca_net != 0)
        
        risk_df["RISQUE_IND_6"] = np.where(has_data, groupe, "Non disponible")
        risk_df["GAP_IND_6"] = np.where(has_data & ~is_vert, ecart, 0.0)
        risk_df["SCORE_IND_6"] = np.where(has_data & ~is_vert, score, 0)
        
        return risk_df

    @staticmethod
    def calculate_indicator_20(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 20: Ratio bénéfice imposable / CA (OPTIMISÉ)
        
        Logique: BENEFICE_IMPOSABLE / CA_HTVA >= 0.2
        Paramètres R: criticite=2, seuil=0.2, coeff=0.5
        
        Complexité: O(n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_20" not in risk_df.columns:
            risk_df["RISQUE_IND_20"] = "Non disponible"
        if "GAP_IND_20" not in risk_df.columns:
            risk_df["GAP_IND_20"] = 0.0
        if "SCORE_IND_20" not in risk_df.columns:
            risk_df["SCORE_IND_20"] = 0

        # Merge efficace
        cols_needed = ["NUM_IFU", "ANNEE", "BENEFICE_IMPOSABLE", "CA_HTVA"]
        existing_cols = [col for col in cols_needed if col in merged_data.columns]
        
        if len(existing_cols) < 4:  # Pas assez de colonnes
            return risk_df
        
        df = risk_df.merge(
            merged_data[existing_cols].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left"
        )
        
        # Paramètres
        criticite = 2
        seuil = 0.2
        coeff = 0.5
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000
        
        # Calcul vectorisé
        benefice = df.get("BENEFICE_IMPOSABLE", pd.Series([np.nan] * len(df))).fillna(0)
        ca_htva = df.get("CA_HTVA", pd.Series([0] * len(df))).fillna(0)
        
        # Indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(ca_htva != 0, benefice / ca_htva, 0)
        
        # Condition vert: seuil < indicateur
        is_vert = indicateur > seuil
        
        # Calcul écart et score
        ecart = np.abs((seuil * ca_htva) - benefice) * coeff
        impact = calculate_impact_vectorized(ecart, x1, x2, x3, x4)
        score = criticite * impact
        
        # Groupe vectorisé
        groupe = get_groupe_vectorized(score, criticite)
        groupe = np.where(is_vert, "vert", groupe)
        
        # Gestion des NA
        has_data = (ca_htva != 0) & df.get("BENEFICE_IMPOSABLE", pd.Series([np.nan] * len(df))).notna()
        
        risk_df["RISQUE_IND_20"] = np.where(has_data, groupe, "Non disponible")
        risk_df["GAP_IND_20"] = np.where(has_data & ~is_vert, ecart, 0.0)
        risk_df["SCORE_IND_20"] = np.where(has_data & ~is_vert, score, 0)
        
        return risk_df

    @staticmethod
    def calculate_indicator_21(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 21: Variation incohérente des clients vs résultats (OPTIMISÉ)
        
        Logique: variation_clients/CA_HTVA < 0 ET variation_resultats/CA > 0 → rouge
        
        Complexité: O(n log n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_21" not in risk_df.columns:
            risk_df["RISQUE_IND_21"] = "Non disponible"

        # Vérifier que les colonnes nécessaires existent
        required_cols = ["BI_Clients_Exer31_12_N_Net", "XB_CA_31_12_N_Net"]
        if not all(col in merged_data.columns for col in required_cols):
            return risk_df

        # Trier et calculer les variations en une seule passe
        df_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"]).copy()
        
        # Variation des clients (vectorisé) O(n)
        df_sorted["variation_clients"] = df_sorted.groupby("NUM_IFU")["BI_Clients_Exer31_12_N_Net"].diff()
        
        # Somme XG + XH (vectorisé)
        xg = df_sorted.get("XG_RESULT_AO_31_12_N_Net", pd.Series([0] * len(df_sorted))).fillna(0)
        xh = df_sorted.get("XH_RESULTAT_HAO_31_12_N_Net", pd.Series([0] * len(df_sorted))).fillna(0)
        df_sorted["SOMME_XG_XH"] = xg + xh
        
        # Variation des résultats (vectorisé)
        df_sorted["variation_resultats"] = df_sorted.groupby("NUM_IFU")["SOMME_XG_XH"].diff()
        
        # Calcul des ratios (vectorisé)
        ca_htva = df_sorted.get("CA_HTVA", pd.Series([0] * len(df_sorted))).fillna(0)
        ca_net = df_sorted.get("XB_CA_31_12_N_Net", pd.Series([0] * len(df_sorted))).fillna(0)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio_21_1 = np.where(
                (ca_htva != 0) & df_sorted["variation_clients"].notna(),
                df_sorted["variation_clients"] / ca_htva,
                0
            )
            ratio_21_2 = np.where(
                (ca_net != 0) & df_sorted["variation_resultats"].notna(),
                df_sorted["variation_resultats"] / ca_net,
                0
            )
        
        # Détermination du risque (vectorisé)
        df_sorted["RISQUE_IND_21"] = np.where(
            (ratio_21_1 < 0) & (ratio_21_2 > 0),
            "rouge",
            "vert"
        )
        
        # Merge avec risk_df O(n log n)
        result = risk_df.merge(
            df_sorted[["NUM_IFU", "ANNEE", "RISQUE_IND_21"]].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left",
            suffixes=('_old', '')
        )
        
        # Gestion des colonnes dupliquées
        if "RISQUE_IND_21_old" in result.columns:
            result = result.drop(columns=["RISQUE_IND_21_old"])
        
        risk_df["RISQUE_IND_21"] = result["RISQUE_IND_21"].fillna("Non disponible")
        
        return risk_df

    @staticmethod
    def calculate_indicator_23(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 23: Variation négative de la marge commerciale (OPTIMISÉ)
        
        Logique: variation_marge_commerciale / CA < 0 → rouge
        
        Complexité: O(n log n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_23" not in risk_df.columns:
            risk_df["RISQUE_IND_23"] = "Non disponible"

        # Vérifier que les colonnes nécessaires existent
        if "XA_MargCommerc_31_12_N_Net" not in merged_data.columns:
            return risk_df

        # Trier et calculer la variation (vectorisé)
        df_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"]).copy()
        
        # Variation de la marge (vectorisé) O(n)
        df_sorted["variation_marge"] = df_sorted.groupby("NUM_IFU")["XA_MargCommerc_31_12_N_Net"].diff()
        
        # Calcul du ratio et risque (vectorisé)
        ca_net = df_sorted.get("XB_CA_31_12_N_Net", pd.Series([0] * len(df_sorted))).fillna(0)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(
                (ca_net != 0) & df_sorted["variation_marge"].notna(),
                df_sorted["variation_marge"] / ca_net,
                np.nan
            )
        
        # Détermination du risque (vectorisé)
        df_sorted["RISQUE_IND_23"] = np.select(
            [
                pd.isna(indicateur) | (ca_net == 0),
                indicateur < 0,
            ],
            [
                "Non disponible",
                "rouge",
            ],
            default="vert"
        )
        
        # Merge avec risk_df O(n log n)
        result = risk_df.merge(
            df_sorted[["NUM_IFU", "ANNEE", "RISQUE_IND_23"]].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left",
            suffixes=('_old', '')
        )
        
        if "RISQUE_IND_23_old" in result.columns:
            result = result.drop(columns=["RISQUE_IND_23_old"])
        
        risk_df["RISQUE_IND_23"] = result["RISQUE_IND_23"].fillna("Non disponible")
        
        return risk_df

    @staticmethod
    def calculate_indicator_24(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 24: Marge commerciale vs médiane sectorielle (OPTIMISÉ)
        
        Logique R:
            1. RATIO_24 = XA_MargCommerc_31_12_N_Net / XB_CA_31_12_N_Net
            2. MEDIAN_RATIO_24 = médiane par (CODE_SECT_ACT, ANNEE_FISCAL)
            3. seuil = MEDIAN_RATIO_24 - 0.1
            4. Si indicateur > seuil → vert, sinon calcul score
        
        Paramètres R: criticite=4, coeff=0.3, x1=500K, x2=5M, x3=20M, x4=100M
        """
        # Paramètres
        criticite = 4
        coeff = 0.3
        seuil_offset = 0.1  # seuil = median - 0.1
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes
        if "RISQUE_IND_24" not in risk_df.columns:
            risk_df["RISQUE_IND_24"] = "Non disponible"
        if "GAP_IND_24" not in risk_df.columns:
            risk_df["GAP_IND_24"] = 0.0
        if "SCORE_IND_24" not in risk_df.columns:
            risk_df["SCORE_IND_24"] = 0

        # Vérifier colonnes nécessaires
        required_cols = ["XA_MARGCOMMERC_31_12_N_NET", "XB_CA_31_12_N_NET", "CODE_SECT_ACT", "ANNEE_FISCAL"]
        # Essayer avec majuscules/minuscules
        col_mapping = {}
        for col in required_cols:
            if col in merged_data.columns:
                col_mapping[col] = col
            elif col.upper() in merged_data.columns:
                col_mapping[col] = col.upper()
            elif col.lower() in merged_data.columns:
                col_mapping[col] = col.lower()
            # Version avec casse mixte
            elif "XA_MargCommerc_31_12_N_Net" in merged_data.columns and col == "XA_MARGCOMMERC_31_12_N_NET":
                col_mapping[col] = "XA_MargCommerc_31_12_N_Net"
            elif "XB_CA_31_12_N_Net" in merged_data.columns and col == "XB_CA_31_12_N_NET":
                col_mapping[col] = "XB_CA_31_12_N_Net"
        
        # Utiliser les colonnes trouvées
        marge_col = col_mapping.get("XA_MARGCOMMERC_31_12_N_NET", "XA_MargCommerc_31_12_N_Net")
        ca_col = col_mapping.get("XB_CA_31_12_N_NET", "XB_CA_31_12_N_Net")
        
        if marge_col not in merged_data.columns or ca_col not in merged_data.columns:
            return risk_df

        df = merged_data.copy()
        
        # Étape 1: Calcul RATIO_24 = XA_MargCommerc / XB_CA (vectorisé)
        marge = pd.to_numeric(df[marge_col], errors="coerce").fillna(0)
        ca = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_24"] = np.where(
                (marge != 0) & (ca != 0) & marge.notna() & ca.notna(),
                marge / ca,
                0
            )
        
        # Étape 2: Calcul de la médiane par secteur et année (vectorisé)
        # Exclure les ratios = 0 pour le calcul de médiane
        df_for_median = df[df["RATIO_24"] != 0].copy()
        
        if len(df_for_median) > 0 and "CODE_SECT_ACT" in df.columns:
            annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in df.columns else "ANNEE"
            median_df = df_for_median.groupby(["CODE_SECT_ACT", annee_col])["RATIO_24"].median().reset_index()
            median_df.columns = ["CODE_SECT_ACT", annee_col, "MEDIAN_RATIO_24"]
            
            # Joindre la médiane au DataFrame principal
            df = df.merge(median_df, on=["CODE_SECT_ACT", annee_col], how="left")
        else:
            df["MEDIAN_RATIO_24"] = np.nan
        
        # Étape 3: Calcul du seuil dynamique = MEDIAN - 0.1
        seuil = df["MEDIAN_RATIO_24"].fillna(0) - seuil_offset
        
        # Étape 4: Calcul de l'indicateur et du risque (vectorisé)
        numerateur = marge
        denominateur = ca
        
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)
        
        # Masque valide
        valid_mask = (
            marge.notna() & ca.notna() & 
            df["MEDIAN_RATIO_24"].notna() & 
            (df["MEDIAN_RATIO_24"] != 0)
        )
        
        # Condition vert: indicateur > seuil
        vert_mask = valid_mask & (indicateur > seuil)
        
        # Condition calcul: indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        
        # Calcul écart: abs((seuil * denominateur) - numerateur) * coeff
        ecart = np.abs((seuil * denominateur) - numerateur) * coeff
        
        # Impact vectorisé
        impact = calculate_impact_vectorized(ecart.values, x1, x2, x3, x4)
        score = criticite * impact
        
        # Groupe vectorisé
        groupe = get_groupe_vectorized(score, criticite)
        
        # Appliquer les résultats
        risk_df.loc[vert_mask, "RISQUE_IND_24"] = "vert"
        risk_df.loc[vert_mask, "GAP_IND_24"] = 0
        risk_df.loc[vert_mask, "SCORE_IND_24"] = 0
        
        risk_df.loc[calc_mask, "RISQUE_IND_24"] = groupe[calc_mask]
        risk_df.loc[calc_mask, "GAP_IND_24"] = ecart[calc_mask].values
        risk_df.loc[calc_mask, "SCORE_IND_24"] = score[calc_mask]

        return risk_df

    @staticmethod
    def calculate_indicator_25(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 25: Valeur ajoutée vs médiane sectorielle (OPTIMISÉ)
        
        Logique R:
            1. RATIO_25 = XC_VALEUR_AJOUTEE_31_12_N_Net / XB_CA_31_12_N_Net
            2. MEDIAN_RATIO_25 = médiane par (CODE_SECT_ACT, ANNEE_FISCAL)
            3. seuil = MEDIAN_RATIO_25 - 0.05
            4. Si indicateur > seuil → vert, sinon calcul score
        
        Paramètres R: criticite=4, coeff=0.2, x1=500K, x2=5M, x3=20M, x4=100M
        """
        # Paramètres
        criticite = 4
        coeff = 0.2
        seuil_offset = 0.05  # seuil = median - 0.05
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes
        if "RISQUE_IND_25" not in risk_df.columns:
            risk_df["RISQUE_IND_25"] = "Non disponible"
        if "GAP_IND_25" not in risk_df.columns:
            risk_df["GAP_IND_25"] = 0.0
        if "SCORE_IND_25" not in risk_df.columns:
            risk_df["SCORE_IND_25"] = 0

        # Chercher les colonnes avec différentes casses
        va_col = None
        ca_col = None
        for col in merged_data.columns:
            if col.upper() == "XC_VALEUR_AJOUTEE_31_12_N_NET":
                va_col = col
            if col.upper() == "XB_CA_31_12_N_NET":
                ca_col = col
        
        if va_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()
        
        # Étape 1: Calcul RATIO_25 = XC_VALEUR_AJOUTEE / XB_CA (vectorisé)
        valeur_ajoutee = pd.to_numeric(df[va_col], errors="coerce").fillna(0)
        ca = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_25"] = np.where(
                (valeur_ajoutee.notna()) & (ca.notna()) & (ca != 0),
                valeur_ajoutee / ca,
                0
            )
        
        # Étape 2: Calcul de la médiane par secteur et année (vectorisé)
        # Exclure les ratios = 0 pour le calcul de médiane
        df_for_median = df[df["RATIO_25"] != 0].copy()
        
        if len(df_for_median) > 0 and "CODE_SECT_ACT" in df.columns:
            annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in df.columns else "ANNEE"
            median_df = df_for_median.groupby(["CODE_SECT_ACT", annee_col])["RATIO_25"].median().reset_index()
            median_df.columns = ["CODE_SECT_ACT", annee_col, "MEDIAN_RATIO_25"]
            
            # Joindre la médiane au DataFrame principal
            df = df.merge(median_df, on=["CODE_SECT_ACT", annee_col], how="left")
        else:
            df["MEDIAN_RATIO_25"] = np.nan
        
        # Étape 3: Calcul du seuil dynamique = MEDIAN - 0.05
        seuil = df["MEDIAN_RATIO_25"].fillna(0) - seuil_offset
        
        # Étape 4: Calcul de l'indicateur et du risque (vectorisé)
        numerateur = valeur_ajoutee
        denominateur = ca
        
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)
        
        # Masque valide
        valid_mask = (
            valeur_ajoutee.notna() & ca.notna() & 
            (ca != 0) &
            df["MEDIAN_RATIO_25"].notna() & 
            (df["MEDIAN_RATIO_25"] != 0)
        )
        
        # Condition vert: indicateur > seuil
        vert_mask = valid_mask & (indicateur > seuil)
        
        # Condition calcul: indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        
        # Calcul écart: abs((seuil * denominateur) - numerateur) * coeff
        ecart = np.abs((seuil * denominateur) - numerateur) * coeff
        
        # Impact vectorisé
        impact = calculate_impact_vectorized(ecart.values, x1, x2, x3, x4)
        score = criticite * impact
        
        # Groupe vectorisé
        groupe = get_groupe_vectorized(score, criticite)
        
        # Appliquer les résultats
        risk_df.loc[vert_mask, "RISQUE_IND_25"] = "vert"
        risk_df.loc[vert_mask, "GAP_IND_25"] = 0
        risk_df.loc[vert_mask, "SCORE_IND_25"] = 0
        
        risk_df.loc[calc_mask, "RISQUE_IND_25"] = groupe[calc_mask]
        risk_df.loc[calc_mask, "GAP_IND_25"] = ecart[calc_mask].values
        risk_df.loc[calc_mask, "SCORE_IND_25"] = score[calc_mask]

        return risk_df

    @staticmethod
    def calculate_indicator_26(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 26: Production totale vs médiane sectorielle (OPTIMISÉ)
        
        Logique R:
            1. MNT_TOTAL_NOTE_32 = TOTAL_PdtionVenduePays_Valeur + TOTAL_PdtionVendueAutrPays_Valeur +
                                   TOTAL_PdtionVendueHorsOHADA_Valeur + TOTAL_PdtionImmobilise_Valeur
            2. RATIO_26 = MNT_TOTAL_NOTE_32 / XB_CA_31_12_N_Net
            3. MEDIAN_RATIO_26 = médiane par (CODE_SECT_ACT, ANNEE_FISCAL)
            4. seuil = MEDIAN_RATIO_26 - 0.1
            5. Si indicateur > seuil → vert, sinon calcul score via IND_24_25_26
            
        Note: Le calcul utilise ID_TtlProductionExercice comme numérateur (équivalent à MNT_TOTAL_NOTE_32)
        
        Paramètres R: criticite=4, coeff=0.2, x1=500K, x2=5M, x3=20M, x4=100M, seuil_offset=0.1
        """
        # Paramètres
        criticite = 4
        coeff = 0.2
        seuil_offset = 0.1  # seuil = median - 0.1
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes
        if "RISQUE_IND_26" not in risk_df.columns:
            risk_df["RISQUE_IND_26"] = "Non disponible"
        if "GAP_IND_26" not in risk_df.columns:
            risk_df["GAP_IND_26"] = 0.0
        if "SCORE_IND_26" not in risk_df.columns:
            risk_df["SCORE_IND_26"] = 0

        # Chercher les colonnes avec différentes casses
        production_col = None
        ca_col = None
        
        # Colonnes possibles pour la production totale
        production_cols_candidates = [
            "ID_TtlProductionExercice", 
            "ID_TTLPRODUCTIONEXERCICE",
            "id_ttlproductionexercice"
        ]
        
        for col in merged_data.columns:
            if col.upper() == "ID_TTLPRODUCTIONEXERCICE":
                production_col = col
                break
        
        for col in merged_data.columns:
            if col.upper() == "XB_CA_31_12_N_NET":
                ca_col = col
                break
        
        if production_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()
        
        # Étape 1: Extraire les valeurs numériques
        production = pd.to_numeric(df[production_col], errors="coerce").fillna(0)
        ca = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)
        
        # Étape 2: Calcul RATIO_26 = production / CA (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_26"] = np.where(
                (production.notna()) & (ca.notna()) & (ca != 0) & (production != 0),
                production / ca,
                0
            )
        
        # Étape 3: Calcul de la médiane par secteur et année (vectorisé)
        # Exclure les ratios = 0 pour le calcul de médiane
        df_for_median = df[df["RATIO_26"] != 0].copy()
        
        if len(df_for_median) > 0 and "CODE_SECT_ACT" in df.columns:
            annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in df.columns else "ANNEE"
            median_df = df_for_median.groupby(["CODE_SECT_ACT", annee_col])["RATIO_26"].median().reset_index()
            median_df.columns = ["CODE_SECT_ACT", annee_col, "MEDIAN_RATIO_26"]
            
            # Joindre la médiane au DataFrame principal
            df = df.merge(median_df, on=["CODE_SECT_ACT", annee_col], how="left")
        else:
            df["MEDIAN_RATIO_26"] = np.nan
        
        # Étape 4: Calcul du seuil dynamique = MEDIAN - 0.1
        seuil = df["MEDIAN_RATIO_26"].fillna(0) - seuil_offset
        
        # Étape 5: Calcul de l'indicateur et du risque (vectorisé)
        numerateur = production
        denominateur = ca
        
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)
        
        # Masque valide: les 3 valeurs doivent être présentes
        valid_mask = (
            production.notna() & ca.notna() & 
            (ca != 0) &
            df["MEDIAN_RATIO_26"].notna() & 
            (df["MEDIAN_RATIO_26"] != 0)
        )
        
        # Condition vert: indicateur > seuil
        vert_mask = valid_mask & (indicateur > seuil)
        
        # Condition calcul: indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        
        # Calcul écart: abs((seuil * denominateur) - numerateur) * coeff
        ecart = np.abs((seuil * denominateur) - numerateur) * coeff
        
        # Impact vectorisé
        impact = calculate_impact_vectorized(ecart.values, x1, x2, x3, x4)
        score = criticite * impact
        
        # Groupe vectorisé
        groupe = get_groupe_vectorized(score, criticite)
        
        # Appliquer les résultats
        risk_df.loc[vert_mask, "RISQUE_IND_26"] = "vert"
        risk_df.loc[vert_mask, "GAP_IND_26"] = 0
        risk_df.loc[vert_mask, "SCORE_IND_26"] = 0
        
        risk_df.loc[calc_mask, "RISQUE_IND_26"] = groupe[calc_mask]
        risk_df.loc[calc_mask, "GAP_IND_26"] = ecart[calc_mask].values
        risk_df.loc[calc_mask, "SCORE_IND_26"] = score[calc_mask]

        return risk_df

    @staticmethod
    def calculate_indicator_27(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 27: Excédent brut d'exploitation négatif (OPTIMISÉ)
        
        Logique: XD_EXCED_BRUT_EXPL_31_12_N_Net < 0 → rouge
        
        Complexité: O(n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_27" not in risk_df.columns:
            risk_df["RISQUE_IND_27"] = "Non disponible"

        # Vérifier que la colonne existe
        if "XD_EXCED_BRUT_EXPL_31_12_N_Net" not in merged_data.columns:
            return risk_df

        # Merge efficace O(n log n)
        df = risk_df.merge(
            merged_data[["NUM_IFU", "ANNEE", "XD_EXCED_BRUT_EXPL_31_12_N_Net"]].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left"
        )
        
        # Calcul vectorisé
        ebe = df["XD_EXCED_BRUT_EXPL_31_12_N_Net"]
        
        risk_df["RISQUE_IND_27"] = np.select(
            [
                ebe.isna(),
                ebe < 0,
            ],
            [
                "Non disponible",
                "rouge",
            ],
            default="vert"
        )
        
        return risk_df

    @staticmethod
    def calculate_indicator_29(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 29: Incohérence dotations amortissements vs actifs immobilisés (OPTIMISÉ)
        
        Logique: variation_dotations > 0 ET variation_actifs_immob < 0 → rouge
        
        Complexité: O(n log n) au lieu de O(n²)
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_29" not in risk_df.columns:
            risk_df["RISQUE_IND_29"] = "Non disponible"

        # Vérifier que les colonnes existent
        required_cols = ["RL_DotAmortProviDep_31_12_N_Net", "AZ_TtlActifImmob_Exer31_12_N_Brut"]
        if not all(col in merged_data.columns for col in required_cols):
            return risk_df

        # Trier et calculer les variations (vectorisé)
        df_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"]).copy()
        
        # Variation des dotations (vectorisé) O(n)
        df_sorted["variation_dotations"] = df_sorted.groupby("NUM_IFU")["RL_DotAmortProviDep_31_12_N_Net"].diff()
        
        # Variation des actifs (vectorisé)
        df_sorted["variation_actifs"] = df_sorted.groupby("NUM_IFU")["AZ_TtlActifImmob_Exer31_12_N_Brut"].diff()
        
        # Détermination du risque (vectorisé)
        df_sorted["RISQUE_IND_29"] = np.where(
            df_sorted["variation_dotations"].notna() & 
            df_sorted["variation_actifs"].notna() &
            (df_sorted["variation_dotations"] > 0) & 
            (df_sorted["variation_actifs"] < 0),
            "rouge",
            "vert"
        )
        
        # Merge avec risk_df O(n log n)
        result = risk_df.merge(
            df_sorted[["NUM_IFU", "ANNEE", "RISQUE_IND_29"]].drop_duplicates(subset=["NUM_IFU", "ANNEE"]),
            on=["NUM_IFU", "ANNEE"],
            how="left",
            suffixes=('_old', '')
        )
        
        if "RISQUE_IND_29_old" in result.columns:
            result = result.drop(columns=["RISQUE_IND_29_old"])
        
        risk_df["RISQUE_IND_29"] = result["RISQUE_IND_29"].fillna("Non disponible")
        
        return risk_df

    @staticmethod
    def calculate_indicator_32(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 32: Cohérence résultat HAO vs cessions d'immobilisations (OPTIMISÉ)
        
        Logique R:
            1. VAR_XH = variation de XH_RESULTAT_HAO_31_12_N_Net par IFU (diff année N vs N-1)
            2. VAR_RO = variation de RO_ValeurCptCessImmob_31_12_N_Net par IFU
            3. Si VAR_XH < 0 ET VAR_RO > 0 → rouge, sinon vert
            
        Interprétation: Détecte les situations suspectes où le résultat HAO (Hors Activités 
        Ordinaires) diminue alors que les cessions d'immobilisations augmentent - incohérence 
        car les cessions devraient normalement améliorer le résultat HAO.
        """
        # Initialiser la colonne
        if "RISQUE_IND_32" not in risk_df.columns:
            risk_df["RISQUE_IND_32"] = "Non disponible"

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

        xh_col = find_col(merged_data, ["XH_RESULTAT_HAO_31_12_N_Net", "XH_RESULTAT_HAO_31_12_N_NET"])
        ro_col = find_col(merged_data, ["RO_ValeurCptCessImmob_31_12_N_Net", "RO_VALEURCPTCESSIMMOB_31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if xh_col is None or ro_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_XH"] = pd.to_numeric(df[xh_col], errors="coerce").fillna(0)
        df["_RO"] = pd.to_numeric(df[ro_col], errors="coerce").fillna(0)

        # Trier par IFU et année pour calculer les variations correctement
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (équivalent à ave(..., FUN = function(x) c(0, diff(x))))
        df["VAR_XH"] = df.groupby(ifu_col)["_XH"].diff().fillna(0)
        df["VAR_RO"] = df.groupby(ifu_col)["_RO"].diff().fillna(0)

        # Détermination du risque (vectorisé)
        # Rouge si VAR_XH < 0 ET VAR_RO > 0
        df["_RISQUE_32"] = np.where(
            (df["VAR_XH"].notna()) & (df["VAR_RO"].notna()) &
            (df["VAR_XH"] < 0) & (df["VAR_RO"] > 0),
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "_RISQUE_32"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
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
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer le résultat
        risk_df["RISQUE_IND_32"] = risk_df["_RISQUE_32"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_32"], errors="ignore")

        return risk_df

    @staticmethod
    def calculate_indicator_33(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 33: Cohérence résultat HAO vs autres produits/charges HAO (OPTIMISÉ)
        
        Logique R:
            1. VAR_XH = variation de XH_RESULTAT_HAO_31_12_N_Net par IFU
            2. VAR_TO = variation de TO_AutresProdHAO_31_12_N_Net par IFU
            3. VAR_RP = variation de RP_AutresChargHAO_31_12_N_Net par IFU
            4. Si VAR_XH < 0 ET VAR_TO < 0 ET VAR_RP > 0 → rouge, sinon vert
            
        Interprétation: Détecte les situations suspectes où:
        - Le résultat HAO diminue (VAR_XH < 0)
        - Les autres produits HAO diminuent (VAR_TO < 0)
        - Les autres charges HAO augmentent (VAR_RP > 0)
        Cette combinaison suggère une manipulation des éléments exceptionnels.
        """
        # Initialiser la colonne
        if "RISQUE_IND_33" not in risk_df.columns:
            risk_df["RISQUE_IND_33"] = "Non disponible"

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

        xh_col = find_col(merged_data, ["XH_RESULTAT_HAO_31_12_N_Net", "XH_RESULTAT_HAO_31_12_N_NET"])
        to_col = find_col(merged_data, ["TO_AutresProdHAO_31_12_N_Net", "TO_AUTRESPRODHAO_31_12_N_NET"])
        rp_col = find_col(merged_data, ["RP_AutresChargHAO_31_12_N_Net", "RP_AUTRESCHARGHAO_31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if xh_col is None or to_col is None or rp_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_XH"] = pd.to_numeric(df[xh_col], errors="coerce").fillna(0)
        df["_TO"] = pd.to_numeric(df[to_col], errors="coerce").fillna(0)
        df["_RP"] = pd.to_numeric(df[rp_col], errors="coerce").fillna(0)

        # Trier par IFU et année pour calculer les variations correctement
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (équivalent à ave(..., FUN = function(x) c(0, diff(x))))
        df["VAR_XH"] = df.groupby(ifu_col)["_XH"].diff().fillna(0)
        df["VAR_TO"] = df.groupby(ifu_col)["_TO"].diff().fillna(0)
        df["VAR_RP"] = df.groupby(ifu_col)["_RP"].diff().fillna(0)

        # Détermination du risque (vectorisé)
        # Rouge si VAR_XH < 0 ET VAR_TO < 0 ET VAR_RP > 0
        df["_RISQUE_33"] = np.where(
            (df["VAR_XH"].notna()) & (df["VAR_TO"].notna()) & (df["VAR_RP"].notna()) &
            (df["VAR_XH"] < 0) & (df["VAR_TO"] < 0) & (df["VAR_RP"] > 0),
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "_RISQUE_33"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
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
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer le résultat
        risk_df["RISQUE_IND_33"] = risk_df["_RISQUE_33"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_33"], errors="ignore")

        return risk_df

    @staticmethod
    def calculate_indicator_34(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 34: Cohérence charges de personnel vs CA (OPTIMISÉ)
        
        Logique R:
            1. VAR_RK = variation de RK_ChargDePersonnel_31_12_N_Net par IFU
            2. VAR_CA_XB = variation de XB_CA_31_12_N_Net par IFU
            3. RATIO_34 = VAR_RK / XB_CA_31_12_N_Net
            4. Si RATIO_34 > 0 ET VAR_RK > 0 ET VAR_CA_XB < 0 → rouge, sinon vert
            
        Interprétation: Détecte les situations suspectes où:
        - Les charges de personnel augmentent (VAR_RK > 0)
        - Le chiffre d'affaires diminue (VAR_CA_XB < 0)
        - Le ratio est positif (RATIO_34 > 0)
        Cette combinaison est économiquement incohérente.
        """
        # Initialiser la colonne
        if "RISQUE_IND_34" not in risk_df.columns:
            risk_df["RISQUE_IND_34"] = "Non disponible"

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

        rk_col = find_col(merged_data, ["RK_ChargDePersonnel_31_12_N_Net", "RK_CHARGDEPERSONNEL_31_12_N_NET"])
        ca_col = find_col(merged_data, ["XB_CA_31_12_N_Net", "XB_CA_31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if rk_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_RK"] = pd.to_numeric(df[rk_col], errors="coerce").fillna(0)
        df["_CA"] = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)

        # Trier par IFU et année pour calculer les variations correctement
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (équivalent à ave(..., FUN = function(x) c(0, diff(x))))
        df["VAR_RK"] = df.groupby(ifu_col)["_RK"].diff().fillna(0)
        df["VAR_CA_XB"] = df.groupby(ifu_col)["_CA"].diff().fillna(0)

        # Calcul du RATIO_34 = VAR_RK / XB_CA (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_34"] = np.where(
                (df["_CA"] != 0) & df["_CA"].notna() & df["VAR_RK"].notna(),
                df["VAR_RK"] / df["_CA"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_34 > 0 ET VAR_RK > 0 ET VAR_CA_XB < 0
        df["_RISQUE_34"] = np.where(
            (df["RATIO_34"].notna()) & (df["VAR_RK"].notna()) & (df["VAR_CA_XB"].notna()) &
            (df["RATIO_34"] > 0) & (df["VAR_RK"] > 0) & (df["VAR_CA_XB"] < 0),
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "_RISQUE_34"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
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
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer le résultat
        risk_df["RISQUE_IND_34"] = risk_df["_RISQUE_34"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_34"], errors="ignore")

        return risk_df

    @staticmethod
    def calculate_all_indicators(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calcule tous les indicateurs de comptabilité.
        Méthode utilitaire pour appeler tous les indicateurs en une fois.
        """
        risk_df = ComptabiliteIndicators.calculate_indicator_9(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_6(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_20(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_21(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_23(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_27(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_29(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_32(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_33(merged_data, risk_df)
        risk_df = ComptabiliteIndicators.calculate_indicator_34(merged_data, risk_df)

        return risk_df
