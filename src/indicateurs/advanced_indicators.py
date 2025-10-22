"""
Indicateurs de risque avancés et spécialisés
Reproduction des autres fonctions du script R
"""

import pandas as pd
from datetime import datetime


class AdvancedIndicators:
    """Classe pour calculer les indicateurs de risque avancés"""

    @staticmethod
    def calculate_indicator_37(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 37: Variation anormale du poste comptable
        Logique: Variation importante des postes comptables clés
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_37" not in risk_df.columns:
            risk_df["RISQUE_IND_37"] = "Non disponible"

        # Calcul des variations par NUM_IFU
        merged_data_sorted = merged_data.sort_values(["NUM_IFU", "ANNEE"])

        # Variation du CA
        merged_data_sorted["variation_ca"] = merged_data_sorted.groupby("NUM_IFU")[
            "XB_CA_31_12_N_Net"
        ].pct_change()

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
                variation_ca = data_row.get("variation_ca", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if (
                    not pd.isna(variation_ca) and abs(variation_ca) > 0.5
                ):  # Variation > 50%
                    risk_df.loc[risk_mask, "RISQUE_IND_37"] = "rouge"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_37"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_38(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 38: Incohérence charges de personnel vs effectifs
        Logique: Charges de personnel anormalement faibles
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_38" not in risk_df.columns:
            risk_df["RISQUE_IND_38"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                charges_personnel = data_row.get("RK_ChargDePersonnel_31_12_N_Net", 0)
                ca_net = data_row.get("XB_CA_31_12_N_Net", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if (
                    ca_net > 0
                    and not pd.isna(charges_personnel)
                    and not pd.isna(ca_net)
                ):
                    ratio_charges = charges_personnel / ca_net
                    if ratio_charges < 0.05:  # Charges < 5% du CA
                        risk_df.loc[risk_mask, "RISQUE_IND_38"] = "rouge"
                    else:
                        risk_df.loc[risk_mask, "RISQUE_IND_38"] = "vert"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_38"] = "Non disponible"

        return risk_df

    @staticmethod
    def calculate_indicator_39(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 39: Résultat HAO anormalement élevé
        Logique: XH_RESULTAT_HAO / CA > 0.2
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_39" not in risk_df.columns:
            risk_df["RISQUE_IND_39"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                resultat_hao = data_row.get("XH_RESULTAT_HAO_31_12_N_Net", 0)
                ca_net = data_row.get("XB_CA_31_12_N_Net", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if ca_net > 0 and not pd.isna(resultat_hao) and not pd.isna(ca_net):
                    ratio_hao = resultat_hao / ca_net
                    if ratio_hao > 0.2:  # Résultat HAO > 20% du CA
                        risk_df.loc[risk_mask, "RISQUE_IND_39"] = "rouge"
                    else:
                        risk_df.loc[risk_mask, "RISQUE_IND_39"] = "vert"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_39"] = "Non disponible"

        return risk_df

    @staticmethod
    def calculate_indicator_46(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 46: Entreprise à CA élevé sans IS
        Logique: CA > seuil mais pas d'IS déclaré
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_46" not in risk_df.columns:
            risk_df["RISQUE_IND_46"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                ca_net = data_row.get("XB_CA_31_12_N_Net", 0)
                is_declare = data_row.get("IMPOT_DU", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if not pd.isna(ca_net) and ca_net > 100000000:  # CA > 100M
                    if pd.isna(is_declare) or is_declare == 0:
                        risk_df.loc[risk_mask, "RISQUE_IND_46"] = "rouge"
                    else:
                        risk_df.loc[risk_mask, "RISQUE_IND_46"] = "vert"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_46"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_47(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 47: Ratio dette/capitaux propres anormal
        Logique: DD_TtlDetFinRessAssim / CP_TtlCptauxPropRessAssim > 2
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_47" not in risk_df.columns:
            risk_df["RISQUE_IND_47"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                dettes_fin = data_row.get("DD_TtlDetFinRessAssim_Exer31_12_N_Net", 0)
                capitaux_propres = data_row.get(
                    "CP_TtlCptauxPropRessAssim_Exer31_12_N_Net", 0
                )

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if (
                    capitaux_propres > 0
                    and not pd.isna(dettes_fin)
                    and not pd.isna(capitaux_propres)
                ):
                    ratio_dette = dettes_fin / capitaux_propres
                    if ratio_dette > 2:  # Dettes > 2 fois les capitaux propres
                        risk_df.loc[risk_mask, "RISQUE_IND_47"] = "rouge"
                    else:
                        risk_df.loc[risk_mask, "RISQUE_IND_47"] = "vert"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_47"] = "Non disponible"

        return risk_df

    @staticmethod
    def calculate_indicator_49(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 49: Provisions anormalement élevées
        Logique: TtlProviRisqCharg / CA > 0.15
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_49" not in risk_df.columns:
            risk_df["RISQUE_IND_49"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                provisions = data_row.get("TtlProviRisqCharg_AnneeN", 0)
                ca_net = data_row.get("XB_CA_31_12_N_Net", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if ca_net > 0 and not pd.isna(provisions) and not pd.isna(ca_net):
                    ratio_provisions = provisions / ca_net
                    if ratio_provisions > 0.15:  # Provisions > 15% du CA
                        risk_df.loc[risk_mask, "RISQUE_IND_49"] = "rouge"
                    else:
                        risk_df.loc[risk_mask, "RISQUE_IND_49"] = "vert"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_49"] = "Non disponible"

        return risk_df

    @staticmethod
    def calculate_indicator_57(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 57: Entreprise sans employé mais avec charges de personnel
        Logique: FLAG_EMPLOYEUR = 'NON' mais RK_ChargDePersonnel > 0
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_57" not in risk_df.columns:
            risk_df["RISQUE_IND_57"] = "Non disponible"

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                flag_employeur = data_row.get("FLAG_EMPLOYEUR", "")
                charges_personnel = data_row.get("RK_ChargDePersonnel_31_12_N_Net", 0)

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if (
                    not pd.isna(flag_employeur)
                    and flag_employeur == "NON"
                    and not pd.isna(charges_personnel)
                    and charges_personnel > 0
                ):
                    risk_df.loc[risk_mask, "RISQUE_IND_57"] = "rouge"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_57"] = "vert"

        return risk_df

    @staticmethod
    def calculate_indicator_58(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 58: Secteur d'activité à risque élevé
        Logique: CODE_SECT_ACT dans liste secteurs à risque
        """
        # Créer automatiquement les colonnes si elles n'existent pas
        if "RISQUE_IND_58" not in risk_df.columns:
            risk_df["RISQUE_IND_58"] = "Non disponible"

        # Secteurs à risque (à adapter selon le contexte)
        secteurs_risque = ["4711", "4719", "6820", "8299", "9609"]

        # Application du calcul avec ciblage précis
        for _, row in risk_df.iterrows():
            num_ifu = row["NUM_IFU"]
            annee = row["ANNEE"]

            # Ciblage précis des données correspondantes
            mask = (merged_data["NUM_IFU"] == num_ifu) & (merged_data["ANNEE"] == annee)
            matched_data = merged_data[mask]

            if not matched_data.empty:
                data_row = matched_data.iloc[0]
                code_secteur = data_row.get("CODE_SECT_ACT", "")

                risk_mask = (risk_df["NUM_IFU"] == num_ifu) & (
                    risk_df["ANNEE"] == annee
                )
                if not pd.isna(code_secteur) and str(code_secteur) in secteurs_risque:
                    risk_df.loc[risk_mask, "RISQUE_IND_58"] = "rouge"
                else:
                    risk_df.loc[risk_mask, "RISQUE_IND_58"] = "vert"

        return risk_df
