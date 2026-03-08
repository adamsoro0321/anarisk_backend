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
import traceback
from .data_loader import DataLoader
import os
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
    OUTPUT_DIR = "../output"
    DATA_DIR = "../data"
    DOCS_DIR = "../docs"

    def __init__(self, data_loader: DataLoader | None = None):
        self.merged_data = pd.DataFrame()
        self.data_loader = data_loader
        self.logger = logging.getLogger(__name__)
        self.output_dir = "../output"
        self.data_dir = "../data"
        self.non_eligible_list = []
        # Tous les indicateurs disponibles dans calculate_all_indicators
        self.indicateurs = [
            # TVA Indicators
            1, 2, 8, 10, 12, 13, 14,
            # Import/Export Indicators
            3, 4, 5, 7,
            # Comptabilité Indicators
            6, 9, 20, 21, 23, 24, 25, 26, 27, 29, 32, 33, 34,
            # Advanced Indicators
            37, 38, 39, 46, 47, 49, 57, 58
        ]
        self.load_reference_data()
    def invoke_data_loader(self):
        """Initialiser le chargeur de données avec les connexions aux bases"""
        # Récupération des données
        merged_data = self.data_loader.run_extract_merge()
        return merged_data 
    def set_external_data(self, data: pd.DataFrame):
        self.merged_data = data.copy()


    def load_reference_data(self):
        """Charger tous les fichiers de référence utilisés par les scripts R"""
        self.logger.info("Chargement des données de référence...")
        try:
            # 1. Liste des non-éligibles (NON_EIGIBLE.xlsx)
            non_eligible_path = f"{self.DOCS_DIR}/NON_ELIGIBLE_final_05012025.xlsx"
            if os.path.exists(non_eligible_path):
                self.non_eligible_df = pd.read_excel(non_eligible_path)
                self.non_eligible_list = (
                    self.non_eligible_df["NUM_IFU"].astype(str).tolist()
                )
                self.logger.info(
                    f"NON_EIGIBLE.xlsx chargé: {len(self.non_eligible_list)} IFU a exclur"
                )
            else:
                self.non_eligible_list = []
                self.logger.warning(f"{self.DOCS_DIR}/NON_EIGIBLE.xlsx non trouvé")

            # 2. Entreprises liées (Entreprises_LIEES.xlsx)
            entreprises_liees_path = f"{self.DOCS_DIR}/Entreprises_LIEES.xlsx"
            if os.path.exists(entreprises_liees_path):
                self.entreprises_liees = pd.read_excel(entreprises_liees_path)
                self.logger.info(
                    f"Entreprises_LIEES.xlsx chargé: {len(self.entreprises_liees)} lignes"
                )
            else:
                self.entreprises_liees = pd.DataFrame()
                self.logger.warning(
                    f"{self.DOCS_DIR}/Entreprises_LIEES.xlsx non trouvé"
                )

        except Exception as e:
            self.logger.error(
                f"Erreur lors du chargement des données de référence: {e}"
            )
    def exclude_non_eligible(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        """
        if not self.non_eligible_list:
            self.logger.warning("Liste des non-éligibles vide, aucun filtrage effectué")
            return data
        
        # S'assurer que la colonne NUM_IFU existe
        if "NUM_IFU" not in data.columns:
            data.columns = data.columns.str.upper()
            
        if "NUM_IFU" not in data.columns:
            self.logger.error("Colonne NUM_IFU non trouvée dans les données")
            return data
        
        initial_count = len(data)
        
        # Filtrer les IFU non éligibles (équivalent à !(NUM_IFU %in% LISTE_NON_EIGIBLE) en R)
        data_filtered = data[~data["NUM_IFU"].astype(str).isin(self.non_eligible_list)]
        
        excluded_count = initial_count - len(data_filtered)
        self.logger.info(f"Exclusion non-éligibles: {excluded_count} contribuables exclus sur {initial_count}")
        
        return data_filtered

    def pre_process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    def data_spliter_regime(self, data: pd.DataFrame, output_dir: str = None) -> Dict[str, pd.DataFrame]:
        """
        Filtre et sépare les données DCF_PROG par régime fiscal (CODE_REG_FISC)
        
        Reproduit la logique R:
            DCF_PROG <- unique(DCF_PROG)
            DCF_PROG_RNI = subset(DCF_PROG, CODE_REG_FISC %in% c("RN"))
            DCF_PROG_RSI = subset(DCF_PROG, CODE_REG_FISC %in% c("RSI"))
            DCF_PROG_ND = subset(DCF_PROG, CODE_REG_FISC %in% c("ND"))
            DCF_PROG_CME = subset(DCF_PROG, CODE_REG_FISC %in% c("CME","CME_RD","CSI"))
            DCF_PROG_CSB = subset(DCF_PROG, CODE_REG_FISC %in% c("CSB"))
        
        Args:
            data: DataFrame DCF_PROG à filtrer
            output_dir: Répertoire de sortie pour les fichiers CSV (optionnel)
            
        Returns:
            Dict contenant les DataFrames filtrés par régime fiscal
        """
        if output_dir is None:
            output_dir = self.output_dir
            
        # Supprimer les doublons (équivalent unique() en R)
        dcf_prog = data.drop_duplicates()
        self.logger.info(f"DCF_PROG après suppression doublons: {len(dcf_prog)} lignes (était {len(data)})")
        
        # S'assurer que la colonne CODE_REG_FISC existe
        if "CODE_REG_FISC" not in dcf_prog.columns:
            dcf_prog.columns = dcf_prog.columns.str.upper()
            
        if "CODE_REG_FISC" not in dcf_prog.columns:
            self.logger.error("Colonne CODE_REG_FISC non trouvée dans les données")
            return {}
        
        # Filtrage par régime fiscal
        result_dict = {}
        
        # DCF_PROG_RNI: Régime Normal d'Imposition
        dcf_prog_rni = dcf_prog[dcf_prog["CODE_REG_FISC"].isin(["RN"])]
        result_dict["DCF_PROG_RNI"] = dcf_prog_rni
        self.logger.info(f"DCF_PROG_RNI: {len(dcf_prog_rni)} contribuables")
        
        # DCF_PROG_RSI: Régime Simplifié d'Imposition
        dcf_prog_rsi = dcf_prog[dcf_prog["CODE_REG_FISC"].isin(["RSI"])]
        result_dict["DCF_PROG_RSI"] = dcf_prog_rsi
        self.logger.info(f"DCF_PROG_RSI: {len(dcf_prog_rsi)} contribuables")
        
        # DCF_PROG_ND: Non Déclarant
        dcf_prog_nd = dcf_prog[dcf_prog["CODE_REG_FISC"].isin(["ND"])]
        result_dict["DCF_PROG_ND"] = dcf_prog_nd
        self.logger.info(f"DCF_PROG_ND: {len(dcf_prog_nd)} contribuables")
        
        # DCF_PROG_CME: Contribution des Micro-Entreprises (CME, CME_RD, CSI)
        dcf_prog_cme = dcf_prog[dcf_prog["CODE_REG_FISC"].isin(["CME", "CME_RD", "CSI"])]
        result_dict["DCF_PROG_CME"] = dcf_prog_cme
        self.logger.info(f"DCF_PROG_CME: {len(dcf_prog_cme)} contribuables")
        
        # DCF_PROG_CSB: Contribution du Secteur Boisson
        dcf_prog_csb = dcf_prog[dcf_prog["CODE_REG_FISC"].isin(["CSB"])]
        result_dict["DCF_PROG_CSB"] = dcf_prog_csb
        self.logger.info(f"DCF_PROG_CSB: {len(dcf_prog_csb)} contribuables")
        
        self.logger.info(f"Données séparées en {len(result_dict)} régimes fiscaux")
        
        return result_dict

    def calculate_all_indicators(
        self,
        merged_data: pd.DataFrame, 
        indicators: list | None = None , 
        date_derniere_vg: str = "2022-12-31", 
        date_derniere_vp: str = "2022-12-31", 
        date_derniere_avis: str = "2022-12-31"
    ) -> pd.DataFrame:
        """
        Calcul de tous les indicateurs de risque en utilisant les classes modulaires
        Reproduit la logique R:
            BD_TVA_Shiny <- DCF_PROG[,c("NUM_IFU","NOM_MINEFID","ETAT","CODE_SECT_ACT",
                        "CODE_REG_FISC","STRUCTURES","ANNEE_FISCAL",
                        "DATE_DERNIERE_VG","DATE_DERNIERE_VP","DATE_DERNIERE_AVIS")]
        Puis ajout progressif des indicateurs calculés.
        """
        if indicators is None:
            indicators = self.indicateurs
        self.logger.info(f"=== CALCUL DE {len(indicators)} INDICATEURS ===")


        # =====================================================================
        # INITIALISATION DE result (équivalent BD_TVA_Shiny en R)
        # Colonnes de base pour l'identification des contribuables
        # =====================================================================

        BASE_COLUMNS = [
            "NUM_IFU",
            "NOM_MINEFID",        # Raison sociale
            "ETAT",               # État du contribuable
            "CODE_SECT_ACT",      # Code secteur d'activité
            "CODE_REG_FISC",      # Régime fiscal (CME, RSI, RNI, etc.)
            "STRUCTURES",         # Structure de rattachement
            "ANNEE_FISCAL",       # Année fiscale
            "DATE_DERNIERE_VG",   # Date dernière vérification générale
            "DATE_DERNIERE_VP",   # Date dernière vérification ponctuelle
            "DATE_DERNIERE_AVIS", # Date dernier avis de vérification
        ]
        merged_data.columns = merged_data.columns.str.upper()
        
        self.logger.info(f"1.avant filtre: {len(merged_data)} lignes")
        
        merged_data = merged_data[(merged_data["DATE_DERNIERE_VG"]<date_derniere_vg) & 
            (merged_data["DATE_DERNIERE_AVIS"]<date_derniere_avis) &
            (merged_data["DATE_DERNIERE_VP"]<date_derniere_vp)]
        
        self.logger.info(f"2.apres filtre: {len(merged_data)} lignes")
        # Sélectionner les colonnes de base disponibles dans merged_data
        available_base_cols = [col for col in BASE_COLUMNS if col in merged_data.columns]

        # Ajouter ANNEE si présent (pour compatibilité)
        if "ANNEE" in merged_data.columns and "ANNEE" not in available_base_cols:
            available_base_cols.append("ANNEE")

        # Réinitialiser les index pour garantir l'alignement entre merged_data et result
        merged_data = merged_data.reset_index(drop=True)

        # Initialiser result avec les colonnes de base (comme BD_TVA_Shiny en R)
        result = merged_data[available_base_cols].copy()

        self.logger.info(f"Colonnes de base initialisées: {len(available_base_cols)} colonnes")
        # ============================ordre important à respecter=========================================
        result = ControleIndicators.calculate_all_indicators(merged_data,result)

        # Indicateurs TVA (1, 2, 8,10, 12, 13, 14)
        if 1 in indicators:
            result = TVAIndicators.calculate_indicator_1(merged_data, result)
        if 2 in indicators:
            result = TVAIndicators.calculate_indicator_2(merged_data, result)
        if 8 in indicators:
            result = TVAIndicators.calculate_indicator_8(merged_data, result)
        if 10 in indicators:
            result = TVAIndicators.calculate_indicator_10(merged_data, result)
        if 12 in indicators:
            result = TVAIndicators.calculate_indicator_12(merged_data, result)
        if 13 in indicators:
            result = TVAIndicators.calculate_indicator_13(merged_data, result)
        if 14 in indicators:
            result = TVAIndicators.calculate_indicator_14(merged_data, result)

        # Indicateurs Import/Export (3, 4, 5)
        if 3 in indicators:
            result = ImportExportIndicators.calculate_indicator_3(merged_data, result)
        if 4 in indicators:
            result = ImportExportIndicators.calculate_indicator_4(merged_data, result)
        if 5 in indicators:
            result = ImportExportIndicators.calculate_indicator_5(merged_data, result)
        if 7 in indicators:
            result = ImportExportIndicators.calculate_indicator_7(merged_data, result)

        # Indicateurs Comptabilité (9,6,20, 21, 23,24,25,26, 27, 29,32,33,34)
        if 9 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_9(merged_data, result)
        if 6 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_6(merged_data, result)
        if 20 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_20(merged_data, result)
        if 21 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_21(merged_data, result)
        if 23 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_23(merged_data, result)
        if 24 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_24(merged_data, result)
        if 25 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_25(merged_data, result)
        if 26 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_26(merged_data, result)
        if 27 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_27(merged_data, result)
        if 29 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_29(merged_data, result)
        if 32 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_32(merged_data, result)
        if 33 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_33(merged_data, result)
        if 34 in indicators:
            result = ComptabiliteIndicators.calculate_indicator_34(merged_data, result)

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

        self.logger.info("Calcul de tous les indicateurs terminé")
        return result

    def run(
        self, 
        data: pd.DataFrame = None,
        indicateurs: list=None,  

    ) -> Dict[str, any]:
        """
        Exécution complète de l'analyse de risque"""

        self.logger.info("=== DÉMARRAGE PROCESSUS: RECUPERATION & CALCUL DES RISQUES ===")
        start_time = time.time()
        try:
            # Récupération des données
            if data is not None:
                merged_data = data.copy()
            else:
                # Appeler set_external_data avant run en mode local
                merged_data = self.invoke_data_loader()

            # Validation des données
            if merged_data is None or merged_data.empty:
                raise ValueError("Aucune donnée n'a pu être récupérée pour l'analyse")

            self.logger.info(f"Données chargées et mergées avec succès: {len(merged_data)} lignes")

            # Calcul des indicateurs
            if indicateurs is None:
                indicateurs = self.indicateurs
            
            results_data = self.calculate_all_indicators(merged_data, indicateurs)

            # Validation des résultats
            if results_data is None or results_data.empty:
                raise ValueError("Le calcul des indicateurs n'a produit aucun résultat")

            elapsed_time = time.time() - start_time

            # Créer le répertoire de sortie s'il n'existe pas
            output_dir = f"{self.data_dir}/risk_contribuables"
            os.makedirs(output_dir, exist_ok=True)

            # Sauvegarde du fichier CSV
            file_path = f"{output_dir}/RISK_INDICATEUR_CONTRIBUABLES_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
            results_data.to_csv(file_path, index=False, sep=";")
            
            self.logger.info(f"=== ANALYSE TERMINÉE EN {elapsed_time:.2f}s ===")
            self.logger.info(f"Résultats sauvegardés dans: {file_path}")

            # Retourner le format attendu par l'app Dash
            return {
                "status": "success",
                "file": file_path,
                "data": results_data.to_dict(orient="records"),
                "nb_contribuables": len(results_data),
                "nb_indicateurs": len(indicateurs),
                "elapsed_time": round(elapsed_time, 2),
                "message": f"Analyse complétée avec succès pour {len(results_data)} contribuables"
            }

        except ValueError as ve:
            self.logger.error(f"Erreur de validation: {ve}")
            self.logger.error(traceback.format_exc())
            return {
                "status": "error",
                "error": str(ve),
                "nb_contribuables": 0,
                "nb_reports": 0,
                "message": f"Erreur de validation: {ve}",
            }

        except Exception as e:
            self.logger.error(f"Erreur lors de l'analyse complète: {e}")
            self.logger.error(traceback.format_exc())

            # Retourner le format d'erreur attendu par l'app
            return {
                "status": "error",
                "error": str(e),
                "nb_contribuables": 0,
                "nb_reports": 0,
                "message": "Erreur lors de l'analyse modulaire",
            }
