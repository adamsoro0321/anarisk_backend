"""
Indicateurs de risque liés à la TVA
Reproduction des fonctions TVA du script R
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

class TVAIndicators:
    """Classe pour calculer les indicateurs de risque TVA"""

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
        """

        def tva_ind1(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4):
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
                ecart = abs((seuil * denominateur) - numerateur)
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

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_1" not in risk_df.columns:
            risk_df["RISQUE_IND_1"] = "Non disponible"
        if "GAP_IND_1" not in risk_df.columns:
            risk_df["GAP_IND_1"] = 0
        if "SCORE_IND_1" not in risk_df.columns:
            risk_df["SCORE_IND_1"] = 0

        # Application du calcul
        for i in range(len(merged_data)):
            montant_tva = merged_data.iloc[i].get("MONTANT_TVA_NET_A_PAYER_25", 0)
            fourn_tva = merged_data.iloc[i].get("CLI_TVA_DEDUCTIBLE_AN", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(montant_tva) and not pd.isna(fourn_tva):
                result = tva_ind1(
                    5,
                    montant_tva,
                    fourn_tva,
                    0.2,
                    0.8,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )
                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_1"] = result[1]
                risk_df.loc[mask, "GAP_IND_1"] = result[0]
                risk_df.loc[mask, "SCORE_IND_1"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_1"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_1"] = 0
                risk_df.loc[mask, "SCORE_IND_1"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_2(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 2: Risque de fausse facture (concentration fin d'année)
        Logique: Déductions TVA Nov-Déc / Déductions TVA Annuelles > 0.5
        """

        def tva_ind2(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4):
            if (
                denominateur != 0
                and not pd.isna(denominateur)
                and not pd.isna(numerateur)
            ):
                indicateur = numerateur / denominateur
            else:
                indicateur = 0

            if indicateur < seuil:
                groupe = "vert"
                ecart = 0
                score = 0
                return [ecart, groupe, score]
            else:
                ecart = abs(numerateur - (seuil * denominateur))
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

                return [ecart, groupe, score]

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_2" not in risk_df.columns:
            risk_df["RISQUE_IND_2"] = "Non disponible"
        if "GAP_IND_2" not in risk_df.columns:
            risk_df["GAP_IND_2"] = 0
        if "SCORE_IND_2" not in risk_df.columns:
            risk_df["SCORE_IND_2"] = 0

        # Application du calcul
        for i in range(len(merged_data)):
            cli_nov_dec = merged_data.iloc[i].get("CLI_TVA_DEDUCTIBLE_NOV_DEC", 0)
            cli_annuel = merged_data.iloc[i].get("CLI_TVA_DEDUCTIBLE_AN", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(cli_nov_dec) and not pd.isna(cli_annuel):
                result = tva_ind2(
                    4,
                    cli_nov_dec,
                    cli_annuel,
                    0.5,
                    0.75,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )

                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_2"] = result[1]
                risk_df.loc[mask, "GAP_IND_2"] = result[0]
                risk_df.loc[mask, "SCORE_IND_2"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_2"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_2"] = 0
                risk_df.loc[mask, "SCORE_IND_2"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_8(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 8: Cohérence montant déclaré TVA vs déductions
        Logique: MONTANT_DECLARE / Fourn_TVA_DEDUCTIBLE >= 0.95
        """

        def tva_ind8(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4):
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
                ecart = abs((seuil * denominateur) - numerateur)
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

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_8" not in risk_df.columns:
            risk_df["RISQUE_IND_8"] = "Non disponible"
        if "GAP_IND_8" not in risk_df.columns:
            risk_df["GAP_IND_8"] = 0
        if "SCORE_IND_8" not in risk_df.columns:
            risk_df["SCORE_IND_8"] = 0

        # Application du calcul
        for i in range(len(merged_data)):
            montant_declare = merged_data.iloc[i].get("MONTANT_DECLARE", 0)
            fourn_tva = merged_data.iloc[i].get("CLI_TVA_DEDUCTIBLE_AN", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(montant_declare) and not pd.isna(fourn_tva):
                result = tva_ind8(
                    4,
                    montant_declare,
                    fourn_tva,
                    0.95,
                    0.75,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )

                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_8"] = result[1]
                risk_df.loc[mask, "GAP_IND_8"] = result[0]
                risk_df.loc[mask, "SCORE_IND_8"] = result[2]
            else:
                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_8"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_8"] = 0
                risk_df.loc[mask, "SCORE_IND_8"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_12(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 12: Cohérence état TVA annuel vs TVA décembre
        Logique: EtatTVA_AnneeN / TVA_DECEMBRE >= 0.2
        """

        def tva_ind12(
            criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4
        ):
            if (
                denominateur != 0
                and not pd.isna(denominateur)
                and not pd.isna(numerateur)
            ):
                indicateur = numerateur / denominateur
            else:
                indicateur = 0

            if seuil < indicateur:
                groupe = "vert"
                ecart = 0
                score = 0
                return [ecart, groupe, score]
            else:
                ecart = abs((seuil * denominateur) - numerateur)
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

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_12" not in risk_df.columns:
            risk_df["RISQUE_IND_12"] = "Non disponible"
        if "GAP_IND_12" not in risk_df.columns:
            risk_df["GAP_IND_12"] = 0
        if "SCORE_IND_12" not in risk_df.columns:
            risk_df["SCORE_IND_12"] = 0

        # Application du calcul
        for i in range(len(merged_data)):
            etat_tva_annee = merged_data.iloc[i].get("EtatTVA_AnneeN", 0)
            tva_decembre = merged_data.iloc[i].get("TVA_DECEMBRE", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(etat_tva_annee) and not pd.isna(tva_decembre):
                result = tva_ind12(
                    3,
                    etat_tva_annee,
                    tva_decembre,
                    0.2,
                    0.5,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )

                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_12"] = result[1]
                risk_df.loc[mask, "GAP_IND_12"] = result[0]
                risk_df.loc[mask, "SCORE_IND_12"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_12"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_12"] = 0
                risk_df.loc[mask, "SCORE_IND_12"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_13(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 13: Cohérence base imposable TVA vs CA
        Logique: MNT_BASE_IMPOSABLE / CA >= 1
        """

        def tva_ind13(
            criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4
        ):
            if (
                denominateur != 0
                and not pd.isna(denominateur)
                and not pd.isna(numerateur)
            ):
                indicateur = numerateur / denominateur
            else:
                indicateur = 0

            if indicateur > seuil:
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

        # Créer les colonnes de résultats si elles n'existent pas
        if "RISQUE_IND_13" not in risk_df.columns:
            risk_df["RISQUE_IND_13"] = "Non disponible"
        if "GAP_IND_13" not in risk_df.columns:
            risk_df["GAP_IND_13"] = 0
        if "SCORE_IND_13" not in risk_df.columns:
            risk_df["SCORE_IND_13"] = 0

        # Calcul du montant de base imposable (selon script R)
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

        # Calcul de la somme de la base imposable (si les champs existent)
        base_imposable_cols = [
            col for col in base_imposable_fields if col in merged_data.columns
        ]
        if base_imposable_cols:
            merged_data["MNT_BASE_IMPOSABLE"] = (
                merged_data[base_imposable_cols].fillna(0).sum(axis=1)
            )
        else:
            merged_data["MNT_BASE_IMPOSABLE"] = 0

        # Application du calcul
        for i in range(len(merged_data)):
            base_imposable = merged_data.iloc[i]["MNT_BASE_IMPOSABLE"]
            ca_net = merged_data.iloc[i].get("XB_CA_31_12_N_Net", 0)
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            if not pd.isna(base_imposable) and not pd.isna(ca_net):
                result = tva_ind13(
                    3,
                    base_imposable,
                    ca_net,
                    1,
                    0.5,
                    500000,
                    5000000,
                    20000000,
                    100000000,
                )
                # Cibler avec NUM_IFU et ANNEE
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_13"] = result[1]
                risk_df.loc[mask, "GAP_IND_13"] = result[0]
                risk_df.loc[mask, "SCORE_IND_13"] = result[2]
            else:
                mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)
                risk_df.loc[mask, "RISQUE_IND_13"] = "Non disponible"
                risk_df.loc[mask, "GAP_IND_13"] = 0
                risk_df.loc[mask, "SCORE_IND_13"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_14(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 14: Contribuable RN/ND avec LA brute = 0
        Logique: MONTANT_TOTAL_LA_BRUTE_15 = 0 pour RN/ND
        """

        # Créer la colonne de résultats si elle n'existe pas
        if "RISQUE_IND_14" not in risk_df.columns:
            risk_df["RISQUE_IND_14"] = "Non disponible"

        # Application du calcul
        for i in range(len(merged_data)):
            montant_total_la = merged_data.iloc[i].get("MONTANT_TOTAL_LA_BRUTE_15", 0)
            code_reg_fisc = merged_data.iloc[i].get("CODE_REG_FISC", "")
            numero_ifu = merged_data.iloc[i].get("NUM_IFU", "")
            annee = merged_data.iloc[i].get("ANNEE", "")

            # Cibler avec NUM_IFU et ANNEE
            mask = (risk_df["NUM_IFU"] == numero_ifu) & (risk_df["ANNEE"] == annee)

            if (
                not pd.isna(montant_total_la)
                and montant_total_la == 0
                and code_reg_fisc in ["RN", "ND"]
            ):
                risk_df.loc[mask, "RISQUE_IND_14"] = "rouge"
            else:
                risk_df.loc[mask, "RISQUE_IND_14"] = "vert"

        return risk_df
