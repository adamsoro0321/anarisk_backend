"""
Indicateurs de risque liés aux importations et exportations
Reproduction des fonctions d'import/export du script R
"""

import pandas as pd
import numpy as np
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
        
        Logique R:
            duree_en_mois <- as.numeric(interval(dateCreation, date_actuelle) / ddays(30))
            if (duree_en_mois <= 12 & importation >= 100000000) {
                Risque <- "rouge"
            } else {
                Risque <- "vert"
            }
        
        Entreprise créée il y a moins de 12 mois avec importations >= 100M
        """
        # Initialiser la colonne si elle n'existe pas
        if "RISQUE_IND_3" not in risk_df.columns:
            risk_df["RISQUE_IND_3"] = "Non disponible"

        # Convertir DATE_IMMAT en datetime (tz-naive)
        date_immat = pd.to_datetime(merged_data["DATE_IMMAT"], errors="coerce")
        
        # Supprimer timezone si présente pour éviter l'erreur:
        # "Cannot compare tz-naive and tz-aware datetime-like objects"
        if date_immat.dt.tz is not None:
            date_immat = date_immat.dt.tz_localize(None)
        
        # Calculer la durée en mois (comme en R: interval / ddays(30))
        # Utiliser datetime tz-naive pour la comparaison
        date_actuelle = pd.Timestamp.now().tz_localize(None) if pd.Timestamp.now().tz is not None else pd.Timestamp.now()
        duree_mois = (date_actuelle - date_immat).dt.days / 30
        
        # Récupérer IMPORT_CAF
        import_caf = pd.to_numeric(merged_data["IMPORT_CAF"], errors="coerce").fillna(0)
        
        # Conditions valides (non NA)
        valid_mask = date_immat.notna() & merged_data["IMPORT_CAF"].notna()
        
        # Condition de risque rouge: durée <= 12 mois ET importation >= 100M
        risque_rouge = (duree_mois <= 12) & (import_caf >= 100000000)
        
        # Appliquer les résultats de manière vectorisée
        # Par défaut "Non disponible", puis "vert" si valide, puis "rouge" si condition
        risk_df.loc[valid_mask, "RISQUE_IND_3"] = "vert"
        risk_df.loc[valid_mask & risque_rouge, "RISQUE_IND_3"] = "rouge"

        return risk_df

    @staticmethod
    def _get_groupe_from_score(score: pd.Series, criticite: int) -> pd.Series:
        """
        Détermine le groupe de risque à partir du score et de la criticité
        Reproduit le switch R pour la détermination du groupe
        """
        groupe = pd.Series("vert", index=score.index)
        
        # Mapping des scores vers les groupes
        groupe = np.where(score.isin([1, 2, 3, 4]), "vert", groupe)
        groupe = np.where(score == 5, np.where(criticite == 1, "vert", "jaune"), groupe)
        groupe = np.where(score == 6, np.where(criticite == 2, "vert", "jaune"), groupe)
        groupe = np.where(score.isin([8, 9]), "jaune", groupe)
        groupe = np.where(score == 10, np.where(criticite == 2, "jaune", "rouge"), groupe)
        groupe = np.where(score.isin([12, 16]), "orange", groupe)
        groupe = np.where(score == 15, np.where(criticite == 3, "orange", "rouge"), groupe)
        groupe = np.where(score.isin([20, 25]), "rouge", groupe)
        
        return pd.Series(groupe, index=score.index)

    @staticmethod
    def calculate_indicator_4(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 4: Importateur dépassant les seuils de régime
        Logique R:
            if (regime == "CME") denominateur <- 15000000
            else if (regime == "RSI") denominateur <- 50000000
            indicateur <- numerateur / denominateur
            if (indicateur < seuil) groupe = "vert"
            else calcul écart et score

        Paramètres: criticite=5, seuil=1, coeff=0.8
        """
        # Paramètres fixes comme en R
        criticite = 5
        seuil = 1
        coeff = 0.8
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes
        if "RISQUE_IND_4" not in risk_df.columns:
            risk_df["RISQUE_IND_4"] = "Non disponible"
        if "GAP_IND_4" not in risk_df.columns:
            risk_df["GAP_IND_4"] = 0.0
        if "SCORE_IND_4" not in risk_df.columns:
            risk_df["SCORE_IND_4"] = 0

        # Récupérer les données
        import_caf = pd.to_numeric(merged_data["IMPORT_CAF"], errors="coerce")
        code_reg_fisc = merged_data["CODE_REG_FISC"]
        
        # Calculer le dénominateur selon le régime fiscal
        denominateur = pd.Series(0.0, index=merged_data.index)
        denominateur = np.where(code_reg_fisc == "CME", 15000000, denominateur)
        denominateur = np.where(code_reg_fisc == "RSI", 50000000, denominateur)
        denominateur = pd.Series(denominateur, index=merged_data.index)
        
        # Masque pour dénominateur invalide (régime autre que CME/RSI)
        invalid_denom_mask = (denominateur == 0) | code_reg_fisc.isna()
        
        # Marquer les cas où le ratio n'est pas calculable (régime non concerné)
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_4"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_4"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_4"] = 0
        
        # Calculer l'indicateur
        indicateur = np.where(denominateur > 0, import_caf / denominateur, 0)
        indicateur = pd.Series(indicateur, index=merged_data.index)
        
        # Masque pour valeurs valides (dénominateur > 0 et import_caf non NA)
        valid_mask = ~invalid_denom_mask & import_caf.notna()
        
        # Condition risque vert: indicateur < seuil
        vert_mask = valid_mask & (indicateur < seuil)
        
        # Condition risque avec score: indicateur >= seuil
        risque_mask = valid_mask & (indicateur >= seuil)
        
        # Calcul de l'écart: abs(numerateur - denominateur) * coeff
        ecart = (np.abs(import_caf - denominateur) * coeff).fillna(0)
        
        # Calcul de l'impact
        impact = pd.Series(0, index=merged_data.index)
        impact = np.where(ecart < x1, 1, impact)
        impact = np.where((ecart >= x1) & (ecart < x2), 2, impact)
        impact = np.where((ecart >= x2) & (ecart < x3), 3, impact)
        impact = np.where((ecart >= x3) & (ecart < x4), 4, impact)
        impact = np.where(ecart >= x4, 5, impact)
        impact = pd.Series(impact, index=merged_data.index)
        
        # Calcul du score
        score = criticite * impact
        
        # Déterminer le groupe pour les cas avec risque
        groupe = ImportExportIndicators._get_groupe_from_score(score, criticite)
        
        # Appliquer les résultats
        # Cas "vert" (indicateur < seuil)
        risk_df.loc[vert_mask, "RISQUE_IND_4"] = "vert"
        risk_df.loc[vert_mask, "GAP_IND_4"] = 0
        risk_df.loc[vert_mask, "SCORE_IND_4"] = 0
        
        # Cas avec risque calculé
        risk_df.loc[risque_mask, "RISQUE_IND_4"] = groupe[risque_mask].values
        risk_df.loc[risque_mask, "GAP_IND_4"] = ecart[risque_mask].values
        risk_df.loc[risque_mask, "SCORE_IND_4"] = score[risque_mask].values

        return risk_df

    @staticmethod
    def calculate_indicator_5(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 5: Cohérence des exportations déclarées TVA vs DGD
        Logique R:
            MNT_EXPORTATION_DECLARE = OP_NTAXBLE_EXPORTATIONS_09 + OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10
            indicateur = MNT_EXPORTATION_DECLARE / EXPORT_CAF
            if (indicateur == seuil) groupe = "vert"  # égalité stricte
            else calcul écart et score
        Paramètres: criticite=3, seuil=1, coeff=0.5
        """
        # Paramètres fixes comme en R
        criticite = 3
        seuil = 1
        coeff = 0.5
        x1, x2, x3, x4 = 500000, 5000000, 20000000, 100000000

        # Initialiser les colonnes
        if "RISQUE_IND_5" not in risk_df.columns:
            risk_df["RISQUE_IND_5"] = "Non disponible"
        if "GAP_IND_5" not in risk_df.columns:
            risk_df["GAP_IND_5"] = 0.0
        if "SCORE_IND_5" not in risk_df.columns:
            risk_df["SCORE_IND_5"] = 0

        # Calculer MNT_EXPORTATION_DECLARE comme en R:
        # mutate(MNT_EXPORTATION_DECLARE = OP_NTAXBLE_EXPORTATIONS_09 + OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10)
        op_exportations_09 = pd.to_numeric(
            merged_data.get("OP_NTAXBLE_EXPORTATIONS_09", 0), errors="coerce"
        ).fillna(0)
        op_autres_comm = pd.to_numeric(
            merged_data.get("OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10", 0), errors="coerce"
        ).fillna(0)
        mnt_exportation_declare = op_exportations_09 + op_autres_comm
        
        # Récupérer EXPORT_CAF (dénominateur)
        export_caf = pd.to_numeric(merged_data["EXPORT_CAF"], errors="coerce")
        
        # Masque pour dénominateur invalide (null ou 0)
        invalid_denom_mask = (pd.isna(export_caf) | (export_caf == 0))
        
        # Marquer les cas où le ratio n'est pas calculable
        risk_df.loc[invalid_denom_mask, "RISQUE_IND_5"] = "Ratio non calculable"
        risk_df.loc[invalid_denom_mask, "GAP_IND_5"] = 0.0
        risk_df.loc[invalid_denom_mask, "SCORE_IND_5"] = 0
        
        # Calculer l'indicateur: MNT_EXPORTATION_DECLARE / EXPORT_CAF
        indicateur = np.where(
            (export_caf != 0) & export_caf.notna() & mnt_exportation_declare.notna(),
            mnt_exportation_declare / export_caf,
            0
        )
        indicateur = pd.Series(indicateur, index=merged_data.index)
        
        # Masque pour valeurs valides (dénominateur non nul et non NA, numérateur non NA)
        valid_mask = ~invalid_denom_mask & mnt_exportation_declare.notna()
        
        # Condition risque vert: indicateur == seuil (égalité stricte comme en R)
        vert_mask = valid_mask & (indicateur == seuil)
        
        # Condition risque avec score: indicateur != seuil
        risque_mask = valid_mask & (indicateur != seuil)
        
        # Calcul de l'écart: abs(numerateur - denominateur) * coeff
        ecart = (np.abs(mnt_exportation_declare - export_caf) * coeff).fillna(0)
        
        # Calcul de l'impact
        impact = pd.Series(0, index=merged_data.index)
        impact = np.where(ecart < x1, 1, impact)
        impact = np.where((ecart >= x1) & (ecart < x2), 2, impact)
        impact = np.where((ecart >= x2) & (ecart < x3), 3, impact)
        impact = np.where((ecart >= x3) & (ecart < x4), 4, impact)
        impact = np.where(ecart >= x4, 5, impact)
        impact = pd.Series(impact, index=merged_data.index)
        
        # Calcul du score
        score = criticite * impact
        
        # Déterminer le groupe pour les cas avec risque
        groupe = ImportExportIndicators._get_groupe_from_score(score, criticite)
        
        # Appliquer les résultats
        # Cas "vert" (indicateur == seuil, égalité stricte)
        risk_df.loc[vert_mask, "RISQUE_IND_5"] = "vert"
        risk_df.loc[vert_mask, "GAP_IND_5"] = 0
        risk_df.loc[vert_mask, "SCORE_IND_5"] = 0
        
        # Cas avec risque calculé (indicateur != seuil)
        risk_df.loc[risque_mask, "RISQUE_IND_5"] = groupe[risque_mask].values
        risk_df.loc[risque_mask, "GAP_IND_5"] = ecart[risque_mask].values
        risk_df.loc[risque_mask, "SCORE_IND_5"] = score[risque_mask].values

        return risk_df

    @staticmethod
    def calculate_indicator_7(
        merged_data: pd.DataFrame, risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        INDICATEUR 7: Nombre de titres d'importation/exportation
        
        Logique R:
            INDICATEUR 7_A: if (EXPORT_NOMBRE_TITRE > 5) "rouge" else "vert"
            INDICATEUR 7_B: if (IMPORT_NOMBRE_TITRE > 5) "rouge" else "vert"
            
        Entreprises avec plus de 5 titres d'export ou d'import
        """
        # Initialiser les colonnes si elles n'existent pas
        if "RISQUE_IND_7_A" not in risk_df.columns:
            risk_df["RISQUE_IND_7_A"] = "Non disponible"
        if "RISQUE_IND_7_B" not in risk_df.columns:
            risk_df["RISQUE_IND_7_B"] = "Non disponible"

        # Récupérer les données
        export_nombre_titre = pd.to_numeric(
            merged_data.get("EXPORT_NOMBRE_TITRE"), errors="coerce"
        )
        import_nombre_titre = pd.to_numeric(
            merged_data.get("IMPORT_NOMBRE_TITRE"), errors="coerce"
        )
        
        # ===== INDICATEUR 7_A: Nombre de titres d'exportation =====
        # Condition valide (non NA)
        valid_mask_7a = export_nombre_titre.notna()
        
        # Condition de risque rouge: EXPORT_NOMBRE_TITRE > 5
        risque_rouge_7a = export_nombre_titre > 5
        
        # Appliquer les résultats
        risk_df.loc[valid_mask_7a, "RISQUE_IND_7_A"] = "vert"
        risk_df.loc[valid_mask_7a & risque_rouge_7a, "RISQUE_IND_7_A"] = "rouge"
        
        # ===== INDICATEUR 7_B: Nombre de titres d'importation =====
        # Note: En R, la condition NA est sur EXPORT_NOMBRE_TITRE mais le test est sur IMPORT_NOMBRE_TITRE
        # Ici on corrige pour vérifier IMPORT_NOMBRE_TITRE pour la cohérence
        valid_mask_7b = import_nombre_titre.notna()
        
        # Condition de risque rouge: IMPORT_NOMBRE_TITRE > 5
        risque_rouge_7b = import_nombre_titre > 5
        
        # Appliquer les résultats
        risk_df.loc[valid_mask_7b, "RISQUE_IND_7_B"] = "vert"
        risk_df.loc[valid_mask_7b & risque_rouge_7b, "RISQUE_IND_7_B"] = "rouge"

        return risk_df
