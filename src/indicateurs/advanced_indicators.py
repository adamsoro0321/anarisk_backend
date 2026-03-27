"""
Indicateurs de risque avancés et spécialisés
Reproduction des autres fonctions du script R
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class AdvancedIndicators:
    """Classe pour calculer les indicateurs de risque avancés"""

    @staticmethod
    def calculate_indicator_37(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 37: Incohérence variation créances clients vs CA (OPTIMISÉ)

        Logique R:
            1. var_BI = variation de BI_Clients_Exer31_12_N_Net par IFU
            2. var_CAHT = variation de XB_CA_31_12_N_Net par IFU
            3. RATIO_IND_37 = var_BI / var_CAHT (si les deux != 0)
            4. Si RATIO_37 < 0 ET var_BI < 0 ET var_CAHT > 0 → rouge, sinon vert

        Interprétation: Détecte les incohérences où les créances clients diminuent
        alors que le CA augmente (ratio négatif), ce qui est suspect.
        """
        logger.info("Start.compute ===>IND37")
        # Initialiser les colonnes
        if "RISQUE_IND_37" not in risk_df.columns:
            risk_df["RISQUE_IND_37"] = "Non disponible"
        if "RATIO_IND_37" not in risk_df.columns:
            risk_df["RATIO_IND_37"] = 0.0

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

        bi_col = find_col(merged_data, ["BI_Clients_Exer31_12_N_Net", "BI_CLIENTS_EXER31_12_N_NET"])
        ca_col = find_col(merged_data, ["XB_CA_31_12_N_Net", "XB_CA_31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if bi_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique et remplacer NA par 0 (comme en R)
        df["_BI"] = pd.to_numeric(df[bi_col], errors="coerce").fillna(0)
        df["_CA"] = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)

        # Trier par IFU et année pour calculer les variations correctement
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (équivalent R: c(NA, diff(x)))
        df["var_BI"] = df.groupby(ifu_col)["_BI"].diff()
        df["var_CAHT"] = df.groupby(ifu_col)["_CA"].diff()

        # Calcul du RATIO_37 = var_BI / var_CAHT (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_IND_37"] = np.where(
                (df["var_CAHT"].notna()) & (df["var_CAHT"] != 0) &
                (df["var_BI"].notna()) & (df["var_BI"] != 0),
                df["var_BI"] / df["var_CAHT"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_37 < 0 ET var_BI < 0 ET var_CAHT > 0
        df["_RISQUE_37"] = np.where(
            (df["RATIO_IND_37"] < 0) & (df["var_BI"] < 0) & (df["var_CAHT"] > 0),
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "RATIO_IND_37", "_RISQUE_37"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger avec risk_df
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map.rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats
        if "RATIO_IND_37_old" in risk_df.columns:
            risk_df = risk_df.drop(columns=["RATIO_IND_37_old"])
        risk_df["RISQUE_IND_37"] = risk_df["_RISQUE_37"].fillna("Non disponible")
        risk_df["RATIO_IND_37"] = risk_df["RATIO_IND_37"].fillna(0)
        risk_df = risk_df.drop(columns=["_RISQUE_37"], errors="ignore")

        logger.info("END.compute ===>IND37")
        return risk_df

    @staticmethod
    def calculate_indicator_38(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 38: Incohérence bénéfice exigible vs variations CA/créances (OPTIMISÉ)
        
        Logique R:
            1. DCF_PROG$RATIO_38 = IBENEF_EXIGIBLE / XB_CA_31_12_N_Net  (stocké dans DCF_PROG)
            2. var_BI = variation de BI_Clients (calculée dans ind 37)
            3. VAR_CA_XB = variation de XB_CA (calculée dans ind 34)
            4. Si RATIO_38 < 0 ET var_BI < 0 ET VAR_CA_XB > 0 → BD_TVA_Shiny$RISQUE_IND_38 = rouge
            
        Note: En R, RATIO_38 est stocké dans DCF_PROG (merged_data), pas dans BD_TVA_Shiny (risk_df)
        """
        logger.info("Start.compute ===>IND38")
        # Initialiser les colonnes - RISQUE dans risk_df uniquement
        if "RISQUE_IND_38" not in risk_df.columns:
            risk_df["RISQUE_IND_38"] = "Non disponible"

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

        ibenef_col = find_col(merged_data, ["IBENEF_EXIGIBLE", "ibenef_exigible"])
        ca_col = find_col(merged_data, ["XB_CA_31_12_N_Net", "XB_CA_31_12_N_NET"])
        bi_col = find_col(merged_data, ["BI_Clients_Exer31_12_N_Net", "BI_CLIENTS_EXER31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if ibenef_col is None or ca_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_IBENEF"] = pd.to_numeric(df[ibenef_col], errors="coerce").fillna(0)
        df["_CA"] = pd.to_numeric(df[ca_col], errors="coerce").fillna(0)
        
        if bi_col:
            df["_BI"] = pd.to_numeric(df[bi_col], errors="coerce").fillna(0)
        else:
            df["_BI"] = 0

        # Trier par IFU et année
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU
        df["var_BI"] = df.groupby(ifu_col)["_BI"].diff().fillna(0)
        df["VAR_CA_XB"] = df.groupby(ifu_col)["_CA"].diff().fillna(0)

        # Calcul du RATIO_38 = IBENEF_EXIGIBLE / XB_CA (vectorisé)
        # Note R: DCF_PROG$RATIO_38 = stocké dans merged_data
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_38"] = np.where(
                (df["_CA"] != 0) & df["_CA"].notna() & df["_IBENEF"].notna(),
                df["_IBENEF"] / df["_CA"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_38 < 0 ET var_BI < 0 ET VAR_CA_XB > 0
        df["_RISQUE_38"] = np.where(
            (df["RATIO_38"] < 0) & 
            (df["var_BI"].notna()) & (df["var_BI"] < 0) & 
            (df["VAR_CA_XB"].notna()) & (df["VAR_CA_XB"] > 0),
            "rouge",
            "vert"
        )

        # Stocker RATIO_38 dans merged_data (comme en R: DCF_PROG$RATIO_38)
        result_map = df[[ifu_col, annee_col, "RATIO_38", "_RISQUE_38"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")

        # Mettre à jour merged_data avec RATIO_38
        annee_merged_col = annee_col
        if "RATIO_38" not in merged_data.columns:
            merged_data["RATIO_38"] = 0.0
        for idx, row in result_map.iterrows():
            mask = (merged_data[ifu_col] == row[ifu_col]) & (merged_data[annee_merged_col] == row[annee_col])
            merged_data.loc[mask, "RATIO_38"] = row["RATIO_38"]

        # Stocker le nombre de lignes original
        original_len = len(risk_df)

        # Merger uniquement RISQUE avec risk_df (pas RATIO)
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map[[ifu_col, annee_col, "_RISQUE_38"]].rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats - seulement RISQUE dans risk_df
        risk_df["RISQUE_IND_38"] = risk_df["_RISQUE_38"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_38"], errors="ignore")

        logger.info("END.compute ===>IND38")
        return risk_df

    @staticmethod
    def calculate_indicator_39(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 39: Incohérence dettes financières vs transferts charges (OPTIMISÉ)
        Logique R:
            1. DCF_PROG$VAR_DD = variation de DD_TtlDetFinRessAssim_Exer31_12_N_Net par IFU
            2. DCF_PROG$VAR_CF = variation de TM_TransfChargFin_31_12_N_Net par IFU
            3. DCF_PROG$RATIO_39 = VAR_DD / CP_TtlCptauxPropRessAssim_Exer31_12_N_Net (stocké dans DCF_PROG)
            4. Si RATIO_39 < 0 ET VAR_CF > 0 → BD_TVA_Shiny$RISQUE_IND_39 = rouge
        Note: En R, RATIO_39 est stocké dans DCF_PROG (merged_data), pas dans BD_TVA_Shiny (risk_df)
        """
        logger.info("Start.compute ===>IND39")
        # Initialiser les colonnes - RISQUE dans risk_df uniquement
        if "RISQUE_IND_39" not in risk_df.columns:
            risk_df["RISQUE_IND_39"] = "Non disponible"

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

        dd_col = find_col(merged_data, ["DD_TtlDetFinRessAssim_Exer31_12_N_Net", "DD_TTLDETFINRESSASSIM_EXER31_12_N_NET"])
        cf_col = find_col(merged_data, ["TM_TransfChargFin_31_12_N_Net", "TM_TRANSFCHARGFIN_31_12_N_NET"])
        cp_col = find_col(merged_data, ["CP_TtlCptauxPropRessAssim_Exer31_12_N_Net", "CP_TTLCPTAUXPROPRESSASSIM_EXER31_12_N_NET"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if dd_col is None or cp_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_DD"] = pd.to_numeric(df[dd_col], errors="coerce").fillna(0)
        df["_CP"] = pd.to_numeric(df[cp_col], errors="coerce").fillna(0)

        if cf_col:
            df["_CF"] = pd.to_numeric(df[cf_col], errors="coerce").fillna(0)
        else:
            df["_CF"] = 0

        # Trier par IFU et année
        df = df.sort_values([ifu_col, annee_col])

        # Calcul des variations par IFU (c(0, diff(x)) en R)
        df["VAR_DD"] = df.groupby(ifu_col)["_DD"].diff().fillna(0)
        df["VAR_CF"] = df.groupby(ifu_col)["_CF"].diff().fillna(0)

        # Calcul du RATIO_39 = VAR_DD / CP (vectorisé)
        # Note R: DCF_PROG$RATIO_39 = stocké dans merged_data
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_39"] = np.where(
                (df["_CP"] != 0) & df["_CP"].notna() & df["VAR_DD"].notna(),
                df["VAR_DD"] / df["_CP"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_39 < 0 ET VAR_CF > 0
        df["_RISQUE_39"] = np.where(
            (df["RATIO_39"].notna()) & (df["RATIO_39"] < 0) &
            (df["VAR_CF"].notna()) & (df["VAR_CF"] > 0),
            "rouge",
            "vert"
        )

        # Stocker RATIO_39 dans merged_data (comme en R: DCF_PROG$RATIO_39)
        result_map = df[[ifu_col, annee_col, "RATIO_39", "_RISQUE_39"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")

        # Mettre à jour merged_data avec RATIO_39
        annee_merged_col = annee_col
        if "RATIO_39" not in merged_data.columns:
            merged_data["RATIO_39"] = 0.0
        for idx, row in result_map.iterrows():
            mask = (merged_data[ifu_col] == row[ifu_col]) & (merged_data[annee_merged_col] == row[annee_col])
            merged_data.loc[mask, "RATIO_39"] = row["RATIO_39"]

        # Stocker le nombre de lignes original
        original_len = len(risk_df)

        # Merger uniquement RISQUE avec risk_df (pas RATIO)
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map[[ifu_col, annee_col, "_RISQUE_39"]].rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )

        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)

        # Appliquer les résultats - seulement RISQUE dans risk_df
        risk_df["RISQUE_IND_39"] = risk_df["_RISQUE_39"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_39"], errors="ignore")

        logger.info("END.compute ===>IND39")
        return risk_df

    @staticmethod
    def calculate_indicator_46(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 46: Ratio dettes financières / CAFG (OPTIMISÉ)
        
        Logique R:
            if("CAFG" %in% names(DCF_PROG) && nrow(DCF_PROG) > 0){
                BD_TVA_Shiny$RATIO_IND_46 = DD_TtlDetFinRessAssim / CAFG
                Si RATIO_46 > 4 → rouge, sinon vert
            }
            
        Interprétation: Détecte les entreprises dont les dettes financières
        dépassent 4 fois leur capacité d'autofinancement global (CAFG).
        """
        logger.info("Start.compute ===>IND46")
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

        dd_col = find_col(merged_data, ["DD_TtlDetFinRessAssim_Exer31_12_N_Net", "DD_TTLDETFINRESSASSIM_EXER31_12_N_NET"])
        cafg_col = find_col(merged_data, ["CAFG", "cafg"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérification R: if("CAFG" %in% names(DCF_PROG) && nrow(DCF_PROG) > 0)
        # Si colonnes absentes ou DataFrame vide, retourner risk_df inchangé
        if cafg_col is None or dd_col is None or len(merged_data) == 0:
            return risk_df
        
        # Initialiser les colonnes seulement si les conditions sont remplies
        if "RISQUE_IND_46" not in risk_df.columns:
            risk_df["RISQUE_IND_46"] = "Non disponible"
        if "RATIO_IND_46" not in risk_df.columns:
            risk_df["RATIO_IND_46"] = 0.0

        df = merged_data.copy()

        # Convertir en numérique
        df["_DD"] = pd.to_numeric(df[dd_col], errors="coerce").fillna(0)
        df["_CAFG"] = pd.to_numeric(df[cafg_col], errors="coerce").fillna(0)

        # Calcul du RATIO_46 = DD / CAFG (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_46"] = np.where(
                (df["_CAFG"] != 0) & df["_CAFG"].notna(),
                df["_DD"] / df["_CAFG"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_46 > 4
        df["_RISQUE_46"] = np.where(
            df["RATIO_46"] > 4,
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "RATIO_46", "_RISQUE_46"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger avec risk_df
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map.rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats
        if "RATIO_46_old" in risk_df.columns:
            risk_df = risk_df.drop(columns=["RATIO_46_old"])
        risk_df["RISQUE_IND_46"] = risk_df["_RISQUE_46"].fillna("Non disponible")
        risk_df["RATIO_IND_46"] = risk_df["RATIO_46"].fillna(0)
        risk_df = risk_df.drop(columns=["_RISQUE_46", "RATIO_46"], errors="ignore")

        logger.info("END.compute ===>IND46")
        return risk_df

    @staticmethod
    def calculate_indicator_47(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 47: Ratio fonds de roulement / besoin de financement (OPTIMISÉ)
        
        Logique R:
            if(all(c("MontantBesoin_Financement", "FondsRoulement_AnneeN") %in% names(DCF_PROG)) && nrow(DCF_PROG) > 0){
                BD_TVA_Shiny$RATIO_IND_47 = FondsRoulement_AnneeN / MontantBesoin_Financement
                Si RATIO_47 < 0.6 → rouge, sinon vert
            }
            
        Interprétation: Détecte les entreprises dont le fonds de roulement
        ne couvre pas suffisamment leur besoin de financement (< 60%).
        """
        logger.info("Start.compute ===>IND47")
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

        fr_col = find_col(merged_data, ["FondsRoulement_AnneeN", "FONDSROULEMENT_ANNEEN"])
        bf_col = find_col(merged_data, ["MontantBesoin_Financement", "MONTANTBESOIN_FINANCEMENT"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérification R: if(all(c("MontantBesoin_Financement", "FondsRoulement_AnneeN") %in% names(DCF_PROG)) && nrow(DCF_PROG) > 0)
        # Si colonnes absentes ou DataFrame vide, retourner risk_df inchangé
        if fr_col is None or bf_col is None or len(merged_data) == 0:
            return risk_df
        
        # Initialiser les colonnes seulement si les conditions sont remplies
        if "RISQUE_IND_47" not in risk_df.columns:
            risk_df["RISQUE_IND_47"] = "Non disponible"
        if "RATIO_IND_47" not in risk_df.columns:
            risk_df["RATIO_IND_47"] = 0.0

        df = merged_data.copy()

        # Convertir en numérique
        df["_FR"] = pd.to_numeric(df[fr_col], errors="coerce").fillna(0)
        df["_BF"] = pd.to_numeric(df[bf_col], errors="coerce").fillna(0)

        # Calcul du RATIO_47 = FondsRoulement / MontantBesoin (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_47"] = np.where(
                (df["_BF"] != 0) & df["_BF"].notna(),
                df["_FR"] / df["_BF"],
                0
            )

        # Détermination du risque (vectorisé)
        # Rouge si RATIO_47 < 0.6
        df["_RISQUE_47"] = np.where(
            df["RATIO_47"] < 0.6,
            "rouge",
            "vert"
        )

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "RATIO_47", "_RISQUE_47"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger avec risk_df
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map.rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats
        if "RATIO_47_old" in risk_df.columns:
            risk_df = risk_df.drop(columns=["RATIO_47_old"])
        risk_df["RISQUE_IND_47"] = risk_df["_RISQUE_47"].fillna("Non disponible")
        risk_df["RATIO_IND_47"] = risk_df["RATIO_47"].fillna(0)
        risk_df = risk_df.drop(columns=["_RISQUE_47", "RATIO_47"], errors="ignore")

        logger.info("END.compute ===>IND47")
        return risk_df

    @staticmethod
    def calculate_indicator_49(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 49: Ratio dettes/capitaux propres vs médiane sectorielle - MINIER (OPTIMISÉ)
        
        Logique R:
            1. DCF_PROG$RATIO_49 = (DD_TtlDetFinRessAssim * 100) / CP_TtlCptauxPropRessAssim (stocké dans DCF_PROG)
            2. DCF_PROG$MEDIAN_RATIO_49 = médiane par (CODE_SECT_ACT, ANNEE_FISCAL)
            3. Si LIBELLE_GR_SECT_ACT == "MINIER" ET ratio > median → BD_TVA_Shiny$RISQUE_IND_49 = rouge
            4. Si non MINIER → "Non disponible"
            
        Note: En R, RATIO_49 et MEDIAN_RATIO_49 sont stockés dans DCF_PROG (merged_data)
        """
        logger.info("Start.compute ===>IND49")
        # Initialiser les colonnes - RISQUE dans risk_df uniquement
        if "RISQUE_IND_49" not in risk_df.columns:
            risk_df["RISQUE_IND_49"] = "Non disponible"

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

        dd_col = find_col(merged_data, ["DD_TtlDetFinRessAssim_Exer31_12_N_Net", "DD_TTLDETFINRESSASSIM_EXER31_12_N_NET"])
        cp_col = find_col(merged_data, ["CP_TtlCptauxPropRessAssim_Exer31_12_N_Net", "CP_TTLCPTAUXPROPRESSASSIM_EXER31_12_N_NET"])
        libelle_col = find_col(merged_data, ["LIBELLE_GR_SECT_ACT", "libelle_gr_sect_act"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if dd_col is None or cp_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_DD"] = pd.to_numeric(df[dd_col], errors="coerce").fillna(0)
        df["_CP"] = pd.to_numeric(df[cp_col], errors="coerce").fillna(0)

        # Calcul du RATIO_49 = (DD * 100) / CP (vectorisé)
        # Note R: DCF_PROG$RATIO_49 = stocké dans merged_data
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_49"] = np.where(
                (df["_CP"] != 0) & df["_CP"].notna(),
                (df["_DD"] * 100) / df["_CP"],
                0
            )

        # Calcul de la médiane par secteur et année (DCF_PROG$MEDIAN_RATIO_49 en R)
        if "CODE_SECT_ACT" in df.columns:
            median_df = df.groupby(["CODE_SECT_ACT", annee_col])["RATIO_49"].median().reset_index()
            median_df.columns = ["CODE_SECT_ACT", annee_col, "MEDIAN_RATIO_49"]
            df = df.merge(median_df, on=["CODE_SECT_ACT", annee_col], how="left")
        else:
            df["MEDIAN_RATIO_49"] = df["RATIO_49"].median()

        # Détermination du risque (vectorisé)
        # Rouge si MINIER ET ratio > median
        if libelle_col and libelle_col in df.columns:
            is_minier = df[libelle_col].str.upper() == "MINIER"
            df["_RISQUE_49"] = np.where(
                is_minier & df["RATIO_49"].notna() & df["MEDIAN_RATIO_49"].notna() & (df["RATIO_49"] > df["MEDIAN_RATIO_49"]),
                "rouge",
                np.where(is_minier, "vert", "Non disponible")
            )
        else:
            df["_RISQUE_49"] = "Non disponible"

        # Stocker RATIO_49 et MEDIAN_RATIO_49 dans merged_data (comme en R: DCF_PROG)
        result_map = df[[ifu_col, annee_col, "RATIO_49", "MEDIAN_RATIO_49", "_RISQUE_49"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Mettre à jour merged_data avec RATIO_49 et MEDIAN_RATIO_49
        annee_merged_col = annee_col
        if "RATIO_49" not in merged_data.columns:
            merged_data["RATIO_49"] = 0.0
        if "MEDIAN_RATIO_49" not in merged_data.columns:
            merged_data["MEDIAN_RATIO_49"] = 0.0
        for idx, row in result_map.iterrows():
            mask = (merged_data[ifu_col] == row[ifu_col]) & (merged_data[annee_merged_col] == row[annee_col])
            merged_data.loc[mask, "RATIO_49"] = row["RATIO_49"]
            merged_data.loc[mask, "MEDIAN_RATIO_49"] = row["MEDIAN_RATIO_49"]
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger uniquement RISQUE avec risk_df (pas RATIO)
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map[[ifu_col, annee_col, "_RISQUE_49"]].rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats - seulement RISQUE dans risk_df
        risk_df["RISQUE_IND_49"] = risk_df["_RISQUE_49"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_49"], errors="ignore")

        logger.info("END.compute ===>IND49")
        return risk_df

    @staticmethod
    def calculate_indicator_57(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 57: Rentabilité des capitaux propres - MINIER (OPTIMISÉ)
        
        Logique R:
            1. indicateur = XE_RESULT_EXPL_31_12_N_Net / CP_TtlCptauxPropRessAssim
            2. Si LIBELLE_GR_SECT_ACT == "MINIER":
               - Si indicateur > 5 → vert, sinon rouge
            3. Si non MINIER → "Non concerné"
            
        Interprétation: Pour le secteur minier, évalue la rentabilité des capitaux propres.
        Un ratio > 5% est considéré comme satisfaisant.
        """
        logger.info("Start.compute ===>IND57")
        # Initialiser les colonnes
        if "RISQUE_IND_57" not in risk_df.columns:
            risk_df["RISQUE_IND_57"] = "Non disponible"

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

        xe_col = find_col(merged_data, ["XE_RESULT_EXPL_31_12_N_Net", "XE_RESULT_EXPL_31_12_N_NET"])
        cp_col = find_col(merged_data, ["CP_TtlCptauxPropRessAssim_Exer31_12_N_Net", "CP_TTLCPTAUXPROPRESSASSIM_EXER31_12_N_NET"])
        libelle_col = find_col(merged_data, ["LIBELLE_GR_SECT_ACT", "libelle_gr_sect_act"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if xe_col is None or cp_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_XE"] = pd.to_numeric(df[xe_col], errors="coerce").fillna(0)
        df["_CP"] = pd.to_numeric(df[cp_col], errors="coerce").fillna(0)

        # Calcul de l'indicateur = XE / CP (vectorisé)
        with np.errstate(divide='ignore', invalid='ignore'):
            df["IND_57"] = np.where(
                (df["_CP"] != 0) & df["_CP"].notna(),
                df["_XE"] / df["_CP"],
                0
            )

        # Détermination du risque (vectorisé)
        # Si MINIER: indicateur > 5 → vert, sinon rouge
        # Si non MINIER: "Non concerné"
        if libelle_col and libelle_col in df.columns:
            is_minier = df[libelle_col].fillna("").str.upper() == "MINIER"
            has_data = df["_XE"].notna() & df["_CP"].notna()
            
            df["_RISQUE_57"] = np.select(
                [
                    is_minier & has_data & (df["IND_57"] > 5),
                    is_minier & has_data & (df["IND_57"] <= 5),
                    is_minier & ~has_data,
                    ~is_minier & df[libelle_col].notna()
                ],
                [
                    "vert",
                    "rouge",
                    "Non disponible",
                    "Non concerné"
                ],
                default="Non disponible"
            )
        else:
            df["_RISQUE_57"] = "Non disponible"

        # Joindre les résultats à risk_df
        result_map = df[[ifu_col, annee_col, "_RISQUE_57"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
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
        risk_df["RISQUE_IND_57"] = risk_df["_RISQUE_57"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_57"], errors="ignore")

        logger.info("END.compute ===>IND57")
        return risk_df

    @staticmethod
    def calculate_indicator_58(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 58: Ratio dettes/capitaux propres - MINIER (OPTIMISÉ)
        
        Logique R:
            1. DCF_PROG$RATIO_58 = (DD_TtlDetFinRessAssim * 100) / CP_TtlCptauxPropRessAssim (stocké dans DCF_PROG)
            2. Si LIBELLE_GR_SECT_ACT == "MINIER":
               - Si RATIO_58 < 1 → BD_TVA_Shiny$RISQUE_IND_58 = rouge, sinon vert
            3. Si non MINIER → "Non concerné"
            
        Note: En R, RATIO_58 est stocké dans DCF_PROG (merged_data), pas dans BD_TVA_Shiny (risk_df)
        """
        logger.info("Start.compute ===>IND58")
        # Initialiser les colonnes - RISQUE dans risk_df uniquement
        if "RISQUE_IND_58" not in risk_df.columns:
            risk_df["RISQUE_IND_58"] = "Non disponible"

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

        dd_col = find_col(merged_data, ["DD_TtlDetFinRessAssim_Exer31_12_N_Net", "DD_TTLDETFINRESSASSIM_EXER31_12_N_NET"])
        cp_col = find_col(merged_data, ["CP_TtlCptauxPropRessAssim_Exer31_12_N_Net", "CP_TTLCPTAUXPROPRESSASSIM_EXER31_12_N_NET"])
        libelle_col = find_col(merged_data, ["LIBELLE_GR_SECT_ACT", "libelle_gr_sect_act"])
        ifu_col = "NUM_IFU"
        annee_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in merged_data.columns else "ANNEE"

        # Vérifier les colonnes requises
        if dd_col is None or cp_col is None:
            return risk_df

        df = merged_data.copy()

        # Convertir en numérique
        df["_DD"] = pd.to_numeric(df[dd_col], errors="coerce").fillna(0)
        df["_CP"] = pd.to_numeric(df[cp_col], errors="coerce").fillna(0)

        # Calcul du RATIO_58 = (DD * 100) / CP (vectorisé)
        # Note R: DCF_PROG$RATIO_58 = stocké dans merged_data
        with np.errstate(divide='ignore', invalid='ignore'):
            df["RATIO_58"] = np.where(
                (df["_CP"] != 0) & df["_CP"].notna(),
                (df["_DD"] * 100) / df["_CP"],
                np.nan
            )

        # Détermination du risque (vectorisé)
        # Si MINIER: ratio < 1 → rouge, sinon vert
        # Si non MINIER: "Non concerné"
        if libelle_col and libelle_col in df.columns:
            is_minier = df[libelle_col].fillna("").str.upper() == "MINIER"
            has_ratio = df["RATIO_58"].notna()
            
            df["_RISQUE_58"] = np.select(
                [
                    is_minier & has_ratio & (df["RATIO_58"] < 1),
                    is_minier & has_ratio & (df["RATIO_58"] >= 1),
                    is_minier & ~has_ratio,
                    ~is_minier & df[libelle_col].notna()
                ],
                [
                    "rouge",
                    "vert",
                    "Non disponible",
                    "Non concerné"
                ],
                default="Non disponible"
            )
        else:
            df["_RISQUE_58"] = "Non disponible"

        # Stocker RATIO_58 dans merged_data (comme en R: DCF_PROG$RATIO_58)
        result_map = df[[ifu_col, annee_col, "RATIO_58", "_RISQUE_58"]].drop_duplicates(subset=[ifu_col, annee_col], keep="first")
        
        # Mettre à jour merged_data avec RATIO_58
        annee_merged_col = annee_col
        if "RATIO_58" not in merged_data.columns:
            merged_data["RATIO_58"] = 0.0
        for idx, row in result_map.iterrows():
            mask = (merged_data[ifu_col] == row[ifu_col]) & (merged_data[annee_merged_col] == row[annee_col])
            merged_data.loc[mask, "RATIO_58"] = row["RATIO_58"]
        
        # Stocker le nombre de lignes original
        original_len = len(risk_df)
        
        # Merger uniquement RISQUE avec risk_df (pas RATIO)
        annee_risk_col = "ANNEE_FISCAL" if "ANNEE_FISCAL" in risk_df.columns else "ANNEE"
        risk_df = risk_df.merge(
            result_map[[ifu_col, annee_col, "_RISQUE_58"]].rename(columns={annee_col: annee_risk_col}),
            on=[ifu_col, annee_risk_col],
            how="left",
            suffixes=('_old', '')
        )
        
        # Vérifier qu'on n'a pas créé de duplicats
        if len(risk_df) != original_len:
            risk_df = risk_df.drop_duplicates(subset=[ifu_col, annee_risk_col], keep="first").reset_index(drop=True)
        
        # Appliquer les résultats - seulement RISQUE dans risk_df
        risk_df["RISQUE_IND_58"] = risk_df["_RISQUE_58"].fillna("Non disponible")
        risk_df = risk_df.drop(columns=["_RISQUE_58"], errors="ignore")

        logger.info("END.compute ===>IND58")
        return risk_df
