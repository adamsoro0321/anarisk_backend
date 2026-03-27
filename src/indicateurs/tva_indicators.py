"""
Indicateurs de risque liés à la TVA
Reproduction des fonctions TVA du script R
Version optimisée avec opérations vectorisées
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


class TVAIndicators:
    """Classe pour calculer les indicateurs de risque TVA"""

    # Lookup table pour détermination du groupe (pré-calculé pour éviter les if/elif répétés)
    _SCORE_GROUPE_MAP = {
        1: "vert", 2: "vert", 3: "vert", 4: "vert",
        8: "jaune", 9: "jaune",
        12: "orange", 16: "orange",
        20: "rouge", 25: "rouge"
    }
    
    # Scores nécessitant vérification de criticité: {score: {criticite: groupe, ...}}
    _SCORE_CRITICITE_MAP = {
        5: {1: "vert"},    # default: jaune
        6: {2: "vert"},    # default: jaune
        10: {2: "jaune"},  # default: rouge
        15: {3: "orange"}  # default: rouge
    }
    _SCORE_CRITICITE_DEFAULT = {
        5: "jaune", 6: "jaune", 10: "rouge", 15: "rouge"
    }

    @staticmethod
    def _determine_groupe_vectorized(scores: np.ndarray, criticite: int) -> np.ndarray:
        """
        Détermine le groupe de manière vectorisée pour un array de scores.
        Beaucoup plus rapide que des if/elif dans une boucle.
        """
        n = len(scores)
        groupes = np.full(n, "vert", dtype=object)
        
        # Scores simples (mapping direct)
        for score_val, groupe_val in TVAIndicators._SCORE_GROUPE_MAP.items():
            groupes[scores == score_val] = groupe_val
        
        # Scores dépendant de criticité
        for score_val, crit_map in TVAIndicators._SCORE_CRITICITE_MAP.items():
            mask = scores == score_val
            if mask.any():
                if criticite in crit_map:
                    groupes[mask] = crit_map[criticite]
                else:
                    groupes[mask] = TVAIndicators._SCORE_CRITICITE_DEFAULT[score_val]
        
        return groupes

    @staticmethod
    def _calculate_impact_vectorized(ecarts: np.ndarray, x1: float, x2: float, x3: float, x4: float) -> np.ndarray:
        """
        Calcule l'impact de manière vectorisée avec np.select (plus rapide que boucles).
        """
        conditions = [
            ecarts < x1,
            ecarts < x2,
            ecarts < x3,
            ecarts < x4
        ]
        choices = [1, 2, 3, 4]
        return np.select(conditions, choices, default=5)

    @staticmethod
    def calculate_risk_score_basic(
        criticite: int,
        numerateur: float,
        denominateur: float,
        seuil: float,
        coeff: float,
        impacts: List[float],
    ) -> dict:
        """Calcul de base du score de risque (logique R commune)"""
        if denominateur != 0 and not pd.isna(denominateur) and not pd.isna(numerateur):
            indicateur = numerateur / denominateur
        else:
            indicateur = 0

        if indicateur < seuil:
            groupe = "vert"
            ecart = 0
            score = 0
            return {"ecart": ecart, "groupe": groupe, "score": score}
        else:
            ecart = abs(numerateur - (seuil * denominateur))
            ecart = ecart * coeff

            if ecart < impacts[0]:
                impact = 1
            elif ecart < impacts[1]:
                impact = 2
            elif ecart < impacts[2]:
                impact = 3
            elif ecart < impacts[3]:
                impact = 4
            else:
                impact = 5

            score = criticite * impact

            # Détermination du groupe selon le score
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

            return {"ecart": ecart, "groupe": groupe, "score": score}

    @staticmethod
    def calculate_indicator_1(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 1: Risque de remboursement indu TVA
        Logique: MONTANT_TVA_NET_A_PAYER / Fourn_TVA_DEDUCTIBLE >= 0.2
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND1")
        # Paramètres
        criticite = 5
        seuil = 0.2
        coeff = 0.8
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_1"] = "Non disponible"
        risk_df["GAP_IND_1"] = 0.0
        risk_df["SCORE_IND_1"] = 0

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["FOURN_TVA_DEDUCTIBLE_AN"]
        numerateur_series = merged_data["MONTANT_TVA_NET_A_PAYER_25"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_1"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_1"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_1"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        numerateur = numerateur_series.fillna(0).values
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & ~pd.isna(numerateur_series).values

        # Condition "vert": indicateur >= seuil
        vert_mask = valid_mask & (indicateur >= seuil)
        risk_df.loc[vert_mask, "RISQUE_IND_1"] = "vert"

        # Condition "calcul": indicateur < seuil
        calc_mask = valid_mask & (indicateur < seuil)
        if calc_mask.any():
            ecart_calc = np.abs((seuil * denominateur[calc_mask]) - numerateur[calc_mask]) * coeff
            risk_df.loc[calc_mask, "GAP_IND_1"] = ecart_calc

            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_1"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_1"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)

        logger.info("End.compute ===>IND1")

        return risk_df

    @staticmethod
    def calculate_indicator_2(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 2: Risque de fausse facture (concentration fin d'année)
        Logique: Déductions TVA Nov-Déc / Déductions TVA Annuelles > 0.5
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND2")
        # Paramètres
        criticite = 4
        seuil = 0.5
        coeff = 0.75
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_2"] = "Non disponible"
        risk_df["GAP_IND_2"] = 0.0
        risk_df["SCORE_IND_2"] = 0

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["CLI_TVA_DEDUCTIBLE_AN"]
        numerateur_series = merged_data["CLI_TVA_DEDUCTIBLE_NOV_DEC"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_2"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_2"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_2"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        numerateur = numerateur_series.fillna(0).values
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & ~pd.isna(numerateur_series).values

        # Condition "vert": indicateur < seuil
        vert_mask = valid_mask & (indicateur < seuil)
        risk_df.loc[vert_mask, "RISQUE_IND_2"] = "vert"

        # Condition "calcul": indicateur >= seuil
        calc_mask = valid_mask & (indicateur >= seuil)
        if calc_mask.any():
            ecart_calc = np.abs(numerateur[calc_mask] - (seuil * denominateur[calc_mask])) * coeff
            risk_df.loc[calc_mask, "GAP_IND_2"] = ecart_calc

            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_2"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_2"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)
        logger.info("END.compute ===>IND2")

        return risk_df

    @staticmethod
    def calculate_indicator_8(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 8: Cohérence montant déclaré TVA vs déductions
        Logique: MONTANT_DECLARE / Fourn_TVA_DEDUCTIBLE >= 0.95
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND8")
        # Paramètres
        criticite = 4
        seuil = 0.95
        coeff = 0.75
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_8"] = "Non disponible"
        risk_df["GAP_IND_8"] = 0.0
        risk_df["SCORE_IND_8"] = 0

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["FOURN_TVA_DEDUCTIBLE_AN"]
        numerateur_series = merged_data["MONTANT_DECLARE"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_8"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_8"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_8"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        numerateur = numerateur_series.fillna(0).values
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & ~pd.isna(numerateur_series).values

        # Condition R: if(seuil < indicateur) → vert
        vert_mask = valid_mask & (seuil < indicateur)
        risk_df.loc[vert_mask, "RISQUE_IND_8"] = "vert"

        # Condition "calcul": indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        if calc_mask.any():
            ecart_calc = np.abs((seuil * denominateur[calc_mask]) - numerateur[calc_mask]) * coeff
            risk_df.loc[calc_mask, "GAP_IND_8"] = ecart_calc

            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_8"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_8"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)
        logger.info("END.compute ===>IND8")

        return risk_df

    @staticmethod
    def calculate_indicator_10(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 10: Cohérence TVA supportée sur importations vs TVA douanes
        
        Logique R: TVA_IND10
        - Si indicateur > seuil (0.95) → vert (cohérence OK)
        - Sinon calcul du score et de l'écart
        
        Paramètres R: criticite=4, seuil=0.95, coeff=0.75, x1=500K, x2=5M, x3=20M, x4=100M
        
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND10")
        # Paramètres
        criticite = 4
        seuil = 0.95
        coeff = 0.75
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_10"] = "Non disponible"
        risk_df["GAP_IND_10"] = 0.0
        risk_df["SCORE_IND_10"] = 0

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["IMPORT_TVA"]
        numerateur_series = merged_data["TVA_SUPPORTE_IMPORT"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_10"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_10"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_10"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        numerateur = numerateur_series.fillna(0).values
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & ~pd.isna(numerateur_series).values

        # Condition R: if(indicateur > seuil) → vert
        vert_mask = valid_mask & (indicateur > seuil)
        risk_df.loc[vert_mask, "RISQUE_IND_10"] = "vert"

        # Condition "calcul": indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        if calc_mask.any():
            ecart_calc = np.abs((seuil * denominateur[calc_mask]) - numerateur[calc_mask]) * coeff
            risk_df.loc[calc_mask, "GAP_IND_10"] = ecart_calc
            
            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_10"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_10"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)
        logger.info("END.compute ===>IND10")
        return risk_df

    @staticmethod
    def calculate_indicator_12(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 12: Cohérence état TVA annuel vs TVA décembre
        Logique: EtatTVA_AnneeN / TVA_DECEMBRE >= 0.2
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND12")
        # Paramètres
        criticite = 3
        seuil = 0.2
        coeff = 0.5
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_12"] = "Non disponible"
        risk_df["GAP_IND_12"] = 0.0
        risk_df["SCORE_IND_12"] = 0

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["TVA_DECEMBRE"]
        numerateur_series = merged_data["ETATTVA_ANNEEN"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_12"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_12"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_12"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        numerateur = numerateur_series.fillna(0).values
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & ~pd.isna(numerateur_series).values

        # Condition: if(seuil < indicateur) → vert
        vert_mask = valid_mask & (seuil < indicateur)
        risk_df.loc[vert_mask, "RISQUE_IND_12"] = "vert"

        # Condition "calcul": indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        if calc_mask.any():
            ecart_calc = np.abs((seuil * denominateur[calc_mask]) - numerateur[calc_mask]) * coeff
            risk_df.loc[calc_mask, "GAP_IND_12"] = ecart_calc
            
            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_12"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_12"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)

        logger.info("END.compute ===>IND12")
        return risk_df

    @staticmethod
    def calculate_indicator_13(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 13: Cohérence base imposable TVA vs CA
        Logique: MNT_BASE_IMPOSABLE / CA >= 1
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND13")
        # Paramètres
        criticite = 3
        seuil = 1
        coeff = 0.5
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes de résultats
        risk_df["RISQUE_IND_13"] = "Non disponible"
        risk_df["GAP_IND_13"] = 0.0
        risk_df["SCORE_IND_13"] = 0

        # Calcul du montant de base imposable (selon script R) - vectorisé
        base_imposable_fields = [
            "OP_TAXBLE_COURANTE_AUTRES_OPERATIONS_TAXABLES_04",
            "OP_TAXBLE_COURANTE_CESSION_IMMO_03",
            "OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02",
            "OP_TAXBLE_COURANTE_OPERATION_TAXABLE_TAUX_10PC_218",
            "OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01",
            "OP_TAXBLE_MARCHE_CDE_10PC_220",
            "OP_TAXBLE_MARCHE_CDE_AUTRES_OP_TAXABLES_08",
            "OP_TAXBLE_MARCHE_CDE_PRESTATION_SERVICES_06",
            "OP_TAXBLE_MARCHE_CDE_TRAVAUX_IMMO_TRVX_PUPLIC_07",
            "OP_TAXBLE_MARCHE_CDE_VENTES_05",
        ]

        # Calcul de la somme de la base imposable (si les champs existent) - vectorisé
        base_imposable_cols = [
            col for col in base_imposable_fields if col in merged_data.columns
        ]
        if base_imposable_cols:
            numerateur = merged_data[base_imposable_cols].fillna(0).sum(axis=1).values
        else:
            numerateur = np.zeros(len(merged_data))

        # Extraire les colonnes nécessaires AVANT fillna pour détecter les valeurs nulles/0
        denominateur_series = merged_data["XB_CA_31_12_N_NET"]
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(denominateur_series).values | (denominateur_series == 0).values)
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_13"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_13"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_13"] = 0

        # Extraire les colonnes nécessaires (vectorisé)
        denominateur = denominateur_series.fillna(0).values

        # Calcul vectorisé de l'indicateur
        with np.errstate(divide='ignore', invalid='ignore'):
            indicateur = np.where(denominateur != 0, numerateur / denominateur, 0)

        # Masque pour valeurs valides (dénominateur non nul et non NA)
        valid_mask = ~invalid_denom_mask

        # Condition: if(indicateur > seuil) → vert
        vert_mask = valid_mask & (indicateur > seuil)
        risk_df.loc[vert_mask, "RISQUE_IND_13"] = "vert"

        # Condition "calcul": indicateur <= seuil
        calc_mask = valid_mask & (indicateur <= seuil)
        if calc_mask.any():
            # Note: Pour ind13, ecart = abs(denominateur - numerateur) * coeff
            ecart_calc = np.abs(denominateur[calc_mask] - numerateur[calc_mask]) * coeff
            risk_df.loc[calc_mask, "GAP_IND_13"] = ecart_calc
            
            impact = TVAIndicators._calculate_impact_vectorized(ecart_calc, x1, x2, x3, x4)
            scores = criticite * impact
            risk_df.loc[calc_mask, "SCORE_IND_13"] = scores
            risk_df.loc[calc_mask, "RISQUE_IND_13"] = TVAIndicators._determine_groupe_vectorized(scores, criticite)

        logger.info("END.compute ===>IND13")
        return risk_df

    @staticmethod
    def calculate_indicator_14(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 14: Contribuable RN/ND avec LA brute = 0
        Logique: MONTANT_TOTAL_LA_BRUTE_15 = 0 pour RN/ND
        Version optimisée avec assignation directe dans risk_df
        """
        logger.info("Start.compute ===>IND14")
        # Initialiser la colonne de résultats
        risk_df["RISQUE_IND_14"] = "vert"

        # Extraire les colonnes nécessaires (vectorisé)
        montant_total_la = merged_data["MONTANT_TOTAL_LA_BRUTE_15"].fillna(1).values  # fillna(1) pour éviter faux positifs
        code_reg_fisc = merged_data["CODE_REG_FISC"].values

        # Calcul vectorisé: rouge si (montant == 0 ET code in ["RN", "ND"])
        is_rn_nd = np.isin(code_reg_fisc, ["RN", "ND"])
        is_zero = montant_total_la == 0
        rouge_mask = is_zero & is_rn_nd
        
        risk_df.loc[rouge_mask, "RISQUE_IND_14"] = "rouge"

        logger.info("END.compute ===>IND14")
        return risk_df

    @staticmethod
    def calculate_all_indicators(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calcule tous les indicateurs TVA.
        Méthode utilitaire pour appeler tous les indicateurs en une fois.
        """
        risk_df = TVAIndicators.calculate_indicator_1(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_2(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_8(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_10(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_12(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_13(merged_data, risk_df)
        risk_df = TVAIndicators.calculate_indicator_14(merged_data, risk_df)

        return risk_df
