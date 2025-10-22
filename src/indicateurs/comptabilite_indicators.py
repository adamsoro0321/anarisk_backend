"""
Indicateurs de risque liés à la comptabilité
Reproduction des fonctions comptables du script R
"""

import pandas as pd


class ComptabiliteIndicators:
    """Classe pour calculer les indicateurs de risque comptabilité"""

    @staticmethod
    def calculate_indicator_20(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 20: Ratio bénéfice imposable / CA
        Logique: BENEFICE_IMPOSABLE / CA_HTVA >= 0.2
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_20" not in risk_df.columns:
            risk_df["RISQUE_IND_20"] = "Non disponible"
        if "GAP_IND_20" not in risk_df.columns:
            risk_df["GAP_IND_20"] = 0
        if "SCORE_IND_20" not in risk_df.columns:
            risk_df["SCORE_IND_20"] = 0

        def tva_ind20(
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

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                benefice_imposable = data_row.get("BENEFICE_IMPOSABLE", 0)
                ca_htva = data_row.get("CA_HTVA", 0)

                if not pd.isna(benefice_imposable) and not pd.isna(ca_htva):
                    result = tva_ind20(
                        2,
                        benefice_imposable,
                        ca_htva,
                        0.2,
                        0.5,
                        500000,
                        5000000,
                        20000000,
                        100000000,
                    )
                    # Mise à jour avec ciblage précis
                    risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                        risk_df["ANNEE"] == annee
                    )
                    risk_df.loc[risk_mask, "RISQUE_IND_20"] = result[1]
                    risk_df.loc[risk_mask, "GAP_IND_20"] = result[0]
                    risk_df.loc[risk_mask, "SCORE_IND_20"] = result[2]
                else:
                    risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                        risk_df["ANNEE"] == annee
                    )
                    risk_df.loc[risk_mask, "RISQUE_IND_20"] = "Non disponible"
                    risk_df.loc[risk_mask, "GAP_IND_20"] = 0
                    risk_df.loc[risk_mask, "SCORE_IND_20"] = 0

        return risk_df

    @staticmethod
    def calculate_indicator_21(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 21: Variation incohérente des clients vs résultats
        Logique: variation_clients < 0 ET variation_resultats > 0
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_21" not in risk_df.columns:
            risk_df["RISQUE_IND_21"] = "Non disponible"

        # Calcul des variations par NUM_IFU (différences année sur année)
        merged_data_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"])

        # Variation des clients
        merged_data_sorted["variation_clients"] = merged_data_sorted.groupby("NUM_IFU")[
            "BI_Clients_Exer31_12_N_Net"
        ].diff()

        # Somme XG + XH (résultats AO + HAO)
        merged_data_sorted["SOMME_XG_XH"] = merged_data_sorted.get(
            "XG_RESULT_AO_31_12_N_Net", 0
        ) + merged_data_sorted.get("XH_RESULTAT_HAO_31_12_N_Net", 0)

        # Variation des résultats
        merged_data_sorted["variation_resultats"] = merged_data_sorted.groupby(
            "NUM_IFU"
        )["SOMME_XG_XH"].diff()

        # Calcul des ratios temporaires
        merged_data_sorted["ratio_21_1"] = 0.0
        merged_data_sorted["ratio_21_2"] = 0.0

        for i in range(len(merged_data_sorted)):
            ca_htva = merged_data_sorted.iloc[i].get("CA_HTVA", 0)
            ca_net = merged_data_sorted.iloc[i].get("XB_CA_31_12_N_Net", 0)
            variation_clients = merged_data_sorted.iloc[i].get("variation_clients", 0)
            variation_resultats = merged_data_sorted.iloc[i].get(
                "variation_resultats", 0
            )

            if ca_htva != 0 and not pd.isna(ca_htva) and not pd.isna(variation_clients):
                merged_data_sorted.iloc[
                    i, merged_data_sorted.columns.get_loc("ratio_21_1")
                ] = variation_clients / ca_htva

            if ca_net != 0 and not pd.isna(ca_net) and not pd.isna(variation_resultats):
                merged_data_sorted.iloc[
                    i, merged_data_sorted.columns.get_loc("ratio_21_2")
                ] = variation_resultats / ca_net

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data_sorted["NUM_IFU"] == num_ifu) & (
                merged_data_sorted["ANNEE"] == annee
            )
            matched_data = merged_data_sorted[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                ratio_21_1 = data_row.get("ratio_21_1", 0)
                ratio_21_2 = data_row.get("ratio_21_2", 0)

                if ratio_21_1 < 0 and ratio_21_2 > 0:
                    risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                        risk_df["ANNEE"] == annee
                    )
                    risk_df.loc[risk_mask, "RISQUE_IND_21"] = "rouge"
                else:
                    risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                        risk_df["ANNEE"] == annee
                    )
                    risk_df.loc[risk_mask, "RISQUE_IND_21"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_23(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 23: Variation négative de la marge commerciale
        Logique: variation_marge_commerciale / CA < 0
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_23" not in risk_df.columns:
            risk_df["RISQUE_IND_23"] = "Non disponible"

        # Calcul de la variation de la marge commerciale par NUM_IFU
        merged_data_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"])
        merged_data_sorted["variation_marge"] = merged_data_sorted.groupby("NUM_IFU")[
            "XA_MargCommerc_31_12_N_Net"
        ].diff()

        def ind_23(numerateur, denominateur):
            if (
                denominateur != 0
                and not pd.isna(denominateur)
                and not pd.isna(numerateur)
            ):
                indicateur = numerateur / denominateur
                if indicateur < 0:
                    return "rouge"
                else:
                    return "vert"
            else:
                return "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data_sorted["NUM_IFU"] == num_ifu) & (
                merged_data_sorted["ANNEE"] == annee
            )
            matched_data = merged_data_sorted[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                variation_marge = data_row.get("variation_marge", 0)
                ca_net = data_row.get("XB_CA_31_12_N_Net", 0)

                result = ind_23(variation_marge, ca_net)
                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                risk_df.loc[risk_mask, "RISQUE_IND_23"] = result

        return risk_df

    @staticmethod
    def calculate_indicator_27(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 27: Excédent brut d'exploitation négatif
        Logique: XD_EXCED_BRUT_EXPL_31_12_N_Net < 0
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_27" not in risk_df.columns:
            risk_df["RISQUE_IND_27"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                ebe = data_row.get("XD_EXCED_BRUT_EXPL_31_12_N_Net", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if not pd.isna(ebe) and ebe < 0:
                    risk_df.loc[risk_mask, "RISQUE_IND_27"] = "rouge"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_27"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_29(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 29: Incohérence dotations amortissements vs actifs immobilisés
        Logique: variation_dotations > 0 ET variation_actifs_immob < 0
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_29" not in risk_df.columns:
            risk_df["RISQUE_IND_29"] = "Non disponible"

        # Calcul des variations par NUM_IFU
        merged_data_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"])

        # Variation des dotations aux amortissements
        merged_data_sorted["variation_dotations"] = merged_data_sorted.groupby(
            "NUM_IFU"
        )["RL_DotAmortProviDep_31_12_N_Net"].diff()

        # Variation des actifs immobilisés
        merged_data_sorted["variation_actifs_immob"] = merged_data_sorted.groupby(
            "NUM_IFU"
        )["AZ_TtlActifImmob_Exer31_12_N_Brut"].diff()

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data_sorted["NUM_IFU"] == num_ifu) & (
                merged_data_sorted["ANNEE"] == annee
            )
            matched_data = merged_data_sorted[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                variation_dotations = data_row.get("variation_dotations", 0)
                variation_actifs = data_row.get("variation_actifs_immob", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if (
                    not pd.isna(variation_dotations)
                    and not pd.isna(variation_actifs)
                    and variation_dotations > 0
                    and variation_actifs < 0
                ):
                    risk_df.loc[risk_mask, "RISQUE_IND_29"] = "rouge"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_29"] = "vert"

        return risk_df
