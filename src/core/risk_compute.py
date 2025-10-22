"""
Module avancé d'analyse des risques fiscaux - Version modulaire
Reproduction fidèle des 3 scripts R de référence avec optimisations Python
Compatible avec tous les fichiers de référence utilisés par les scripts R
"""

import pandas as pd
import time
import logging
from typing import Dict
import warnings

from .data_loader import DataLoader

# Import des modules d'indicateurs
from indicateurs import (
    TVAIndicators,
    ImportExportIndicators,
    ComptabiliteIndicators,
    ControleIndicators,
    AdvancedIndicators,
)


warnings.filterwarnings("ignore")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../output/risk_compute.log", mode="a"),
    ],
)


class RiskComputer:
    """
    Analyseur avancé de risques fiscaux basé
    Reproduit fidèlement les 58+ indicateurs de risque
    Version modulaire utilisant les classes d'indicateurs spécialisées
    """

    def __init__(self, data_loader: DataLoader | None = None):
        self.merged_data = pd.DataFrame()
        self.data_loader = data_loader
        self.logger = logging.getLogger(__name__)

    def invoke_data_loader(self):
        """Initialiser le chargeur de données avec les connexions aux bases"""

    def compute_from_merged_data(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        pass

    def calculate_all_indicators(
        self, merged_data: pd.DataFrame, indicators: list, nbr_lignes: int | None = None
    ) -> pd.DataFrame:
        """
        Calcul de tous les indicateurs de risque en utilisant les classes modulaires
        """
        self.logger.info("=== CALCUL DE TOUS LES INDICATEURS ===")

        result = merged_data[["NUM_IFU", "ANNEE"]].copy()

        if nbr_lignes is not None:
            merged_data = merged_data.head(nbr_lignes)

        # Indicateurs TVA (1, 2, 8, 12, 13, 14)
        if 1 in indicators:
            result = TVAIndicators.calculate_indicator_1(merged_data, result)
        if 2 in indicators:
            result = TVAIndicators.calculate_indicator_2(merged_data, result)
        if 8 in indicators:
            result = TVAIndicators.calculate_indicator_8(merged_data, result)
        if 12 in indicators:
            result = TVAIndicators.calculate_indicator_12(merged_data, result)
        if 13 in indicators:
            result = TVAIndicators.calculate_indicator_13(merged_data, result)
        if 14 in indicators:
            result = TVAIndicators.calculate_indicator_14(merged_data, result)
        # merged_data = TVAIndicators.calculate_indicator_2(merged_data)
        # merged_data = TVAIndicators.calculate_indicator_8(merged_data)
        # merged_data = TVAIndicators.calculate_indicator_12(merged_data)
        # merged_data = TVAIndicators.calculate_indicator_13(merged_data)
        # merged_data = TVAIndicators.calculate_indicator_14(merged_data)

        # Indicateurs Import/Export (3, 4, 5)
        if 3 in indicators:
            result = ImportExportIndicators.calculate_indicator_3(merged_data, result)
        if 4 in indicators:
            result = ImportExportIndicators.calculate_indicator_4(merged_data, result)
        if 5 in indicators:
            result = ImportExportIndicators.calculate_indicator_5(merged_data, result)
        # merged_data = ImportExportIndicators.calculate_indicator_3(merged_data)
        # merged_data = ImportExportIndicators.calculate_indicator_4(merged_data)
        # merged_data = ImportExportIndicators.calculate_indicator_5(merged_data)

        # Indicateurs Comptabilité (20, 21, 23, 27, 29)
        if 20 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_20(merged_data, result)
        if 21 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_21(merged_data, result)
        if 23 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_23(merged_data, result)
        if 27 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_27(merged_data, result)
        if 29 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_29(merged_data, result)

        # merged_data = ComptabiliteIndicators.calculate_indicator_20(merged_data)
        # merged_data = ComptabiliteIndicators.calculate_indicator_21(merged_data)
        # merged_data = ComptabiliteIndicators.calculate_indicator_23(merged_data)
        # merged_data = ComptabiliteIndicators.calculate_indicator_27(merged_data)
        # merged_data = ComptabiliteIndicators.calculate_indicator_29(merged_data)

        # Indicateurs Contrôle (15, 16)
        if 15 in indicators:
            result = ControleIndicators.calculate_indicator_15(merged_data, result)
        if 16 in indicators:
            result = ControleIndicators.calculate_indicator_16(merged_data, result)
        # merged_data = ControleIndicators.calculate_indicator_15(merged_data)
        # merged_data = ControleIndicators.calculate_indicator_16(merged_data)

        # Indicateurs Avancés (37, 38, 39, 46, 47, 49, 57, 58)
        if 37 in indicators:
            result = AdvancedIndicators.calculate_indicator_37(merged_data, result)
        if 38 in indicators:
            result = AdvancedIndicators.calculate_indicator_38(merged_data, result)
        if 39 in indicators:
            result = AdvancedIndicators.calculate_indicator_39(merged_data, result)
        if 46 in indicators:
            result = AdvancedIndicators.calculate_indicator_46(merged_data, result)
        if 47 in indicators:
            result = AdvancedIndicators.calculate_indicator_47(merged_data, result)
        if 49 in indicators:
            result = AdvancedIndicators.calculate_indicator_49(merged_data, result)
        if 57 in indicators:
            result = AdvancedIndicators.calculate_indicator_57(merged_data, result)
        if 58 in indicators:
            result = AdvancedIndicators.calculate_indicator_58(merged_data, result)
        # merged_data = AdvancedIndicators.calculate_indicator_37(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_38(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_39(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_46(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_47(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_49(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_57(merged_data)
        # merged_data = AdvancedIndicators.calculate_indicator_58(merged_data)

        self.logger.info("Calcul de tous les indicateurs terminé")
        return result

    def run_complete_analysis(self, export_results: bool = True) -> Dict[str, any]:
        """
        Exécution complète de l'analyse de risque
        Compatible avec l'interface de l'application Dash
        """
        self.logger.info("=== DÉMARRAGE ANALYSE COMPLÈTE (MODULAIRE) ===")
        start_time = time.time()

        try:
            # Récupération des données
            merged_data = self.data_loader.run_full_analysis()

            # 3. Calcul des indicateurs
            results_data = self.calculate_all_indicators(merged_data)

            elapsed_time = time.time() - start_time
            nb_contribuables = len(results_data)

            self.logger.info(f"=== ANALYSE TERMINÉE EN {elapsed_time:.2f}s ===")
            self.logger.info(f"Contribuables analysés: {nb_contribuables}")

            # Retourner le format attendu par l'app Dash
            return {
                "success": True,
                "data": results_data if export_results else None,
                "nb_contribuables": nb_contribuables,
                "nb_reports": 0,
                "message": "Analyse modulaire terminée avec succès",
            }

        except Exception as e:
            self.logger.error(f"Erreur lors de l'analyse complète: {e}")
            import traceback

            self.logger.error(traceback.format_exc())

            # Retourner le format d'erreur attendu par l'app
            return {
                "success": False,
                "error": str(e),
                "nb_contribuables": 0,
                "nb_reports": 0,
                "message": "Erreur lors de l'analyse modulaire",
            }
