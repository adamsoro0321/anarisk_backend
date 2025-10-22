"""
Module avancé d'analyse des risques fiscaux - Version modulaire
Reproduction fidèle des 3 scripts R de référence avec optimisations Python
Compatible avec tous les fichiers de référence utilisés par les scripts R
"""

import pandas as pd
import os
import time
import logging
from typing import Dict
from sqlalchemy.engine import Engine
from datetime import datetime
import warnings

# Import des modules d'indicateurs
from .indicateurs import (
    TVAIndicators,
    ImportExportIndicators,
    ComptabiliteIndicators,
    ControleIndicators,
    AdvancedIndicators,
)
from .queries import (
    sql_contribuable,
    sql_tva_complete,
    sql_tva_deduction,
    sql_dgd,
    sql_programmations_control,
    sql_benefices_complete,
)

warnings.filterwarnings("ignore")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("output/risk_analysis_advanced.log", mode="a"),
    ],
)


class AdvancedRiskAnalyzer:
    """
    Analyseur avancé de risques fiscaux basé sur les scripts R
    Reproduit fidèlement les 58+ indicateurs de risque
    Version modulaire utilisant les classes d'indicateurs spécialisées
    """
    IS_LOCAL_MODE = True

    def __init__(self, oracle_engine: Engine, postgres_engine: Engine = None):
        self.oracle_engine = oracle_engine
        self.postgres_engine = postgres_engine
        self.logger = logging.getLogger(__name__)

        # Charger les fichiers de référence
        self.load_reference_data()

    def load_reference_data(self):
        """Charger tous les fichiers de référence utilisés par les scripts R"""
        self.logger.info("Chargement des données de référence...")

        try:
            # 1. Liste des non-éligibles (NON_EIGIBLE.xlsx)
            non_eligible_path = "docs/NON_EIGIBLE.xlsx"
            if os.path.exists(non_eligible_path):
                self.non_eligible_df = pd.read_excel(non_eligible_path)
                self.non_eligible_list = (
                    self.non_eligible_df["NUM_IFU"].astype(str).tolist()
                )
                self.logger.info(
                    f"NON_EIGIBLE.xlsx chargé: {len(self.non_eligible_list)} IFU exclus"
                )
            else:
                self.non_eligible_list = []
                self.logger.warning("NON_EIGIBLE.xlsx non trouvé")

            # 2. Entreprises liées (Entreprises_LIEES.xlsx)
            entreprises_liees_path = "docs/Entreprises_LIEES.xlsx"
            if os.path.exists(entreprises_liees_path):
                self.entreprises_liees = pd.read_excel(entreprises_liees_path)
                self.logger.info(
                    f"Entreprises_LIEES.xlsx chargé: {len(self.entreprises_liees)} lignes"
                )
            else:
                self.entreprises_liees = pd.DataFrame()
                self.logger.warning("Entreprises_LIEES.xlsx non trouvé")

            # Autres fichiers de référence...
            self._load_other_reference_files()

        except Exception as e:
            self.logger.error(
                f"Erreur lors du chargement des données de référence: {e}"
            )
            self._initialize_empty_references()

    def _load_other_reference_files(self):
        """Charger les autres fichiers de référence"""
        reference_files = {
            "meta_donnees": "docs/META_DONNEES.xlsx",
            "fiche_pistes": "docs/FICHE_PISTES_INVESTIGATION.xlsx",
            "nomenclature_sh": "docs/NomenclatureSH.xlsx",
            "base_donnees": "docs/BASE_DONNEES.xlsx",
            "dcf_activites": "docs/DCF_ACTIVITES.xlsx",
        }

        for attr_name, file_path in reference_files.items():
            if os.path.exists(file_path):
                setattr(self, attr_name, pd.read_excel(file_path))
                self.logger.info(f"{file_path} chargé")
            else:
                setattr(self, attr_name, pd.DataFrame())
                self.logger.warning(f"{file_path} non trouvé")

    def _initialize_empty_references(self):
        """Initialiser avec des DataFrames vides en cas d'erreur"""
        self.non_eligible_list = []
        self.entreprises_liees = pd.DataFrame()
        self.meta_donnees = pd.DataFrame()
        self.fiche_pistes = pd.DataFrame()
        self.nomenclature_sh = pd.DataFrame()
        self.base_donnees = pd.DataFrame()
        self.dcf_activites = pd.DataFrame()

    def extract_complete_oracle_data(self) -> Dict[str, pd.DataFrame]:
        """
        Extraction complète des données Oracle selon la logique des scripts R
        Reproduit l'extraction du script 00_Extraction_transformation_v2.R
        """
        self.logger.info("=== EXTRACTION COMPLÈTE ORACLE (Reproduction Scripts R) ===")

        with self.oracle_engine.connect() as connection:
            data = {}

            # 1. Extraction des contribuables (avec exclusions)
            data["contribuables"] = self._extract_contribuables_with_exclusions(
                connection
            )

            # 2. Construction de BD_TVA complète (reproduction logique R)
            data["bd_tva"] = self._construct_bd_tva_complete(connection)

            # 3. Extraction des déductions TVA détaillées
            data["tva_deductions"] = self._extract_dcf_tva_deduction(connection)

            # 4. Données douanières DGD complètes
            data["dgd_data"] = self._extract_dgd_complete_with_nomenclature(connection)

            # 5. Données de programmation et contrôles
            data["programmation"] = self._extract_programmation_controles(connection)

            # 6. Extraction bénéfices (IBNC, IBICA, IS)
            data["benefices"] = self._extract_benefices_complete(connection)

            return data

    def _extract_contribuables_with_exclusions(self, connection) -> pd.DataFrame:
        """Extraction des contribuables avec toutes les exclusions R"""
        self.logger.info("Extraction contribuables avec exclusions (logique R)")

        start_time = time.time()

        # Requête de base sans exclusions
        # Extraire tous les contribuables d'abord
        if self.IS_LOCAL_MODE:
            contribuables = pd.read_csv("data/contribuables.csv")
        else:
            contribuables = pd.read_sql(sql_contribuable, connection)
            # save in data/contribuables.csv
            contribuables.to_csv(
                "data/contribuables.csv", index=False, encoding="utf-8"
            )

        contribuables.columns = contribuables.columns.str.upper()
        initial_count = len(contribuables)

        # Filtrage additionnel pour reproduire la logique R
        contribuables = contribuables[contribuables["NUM_IFU"].notna()]
        contribuables = contribuables[contribuables["NUM_IFU"].str.strip() != ""]

        # Appliquer les exclusions en Python pour éviter la limite Oracle IN (1000)
        if self.non_eligible_list:
            before_exclusion = len(contribuables)
            # Convertir en set pour une recherche plus rapide
            non_eligible_set = set(self.non_eligible_list)
            contribuables = contribuables[
                ~contribuables["NUM_IFU"].isin(non_eligible_set)
            ]
            after_exclusion = len(contribuables)

            self.logger.info(
                f"Exclusions appliquées: {before_exclusion - after_exclusion} IFU exclus "
                f"({len(self.non_eligible_list)} dans la liste d'exclusion)"
            )

        elapsed_time = time.time() - start_time
        final_count = len(contribuables)

        self.logger.info(
            f"Contribuables extraits: {final_count}/{initial_count} après exclusions en {elapsed_time:.2f}s"
        )
        return contribuables

    def _construct_bd_tva_complete(self, connection) -> pd.DataFrame:
        """Construction complète de BD_TVA selon la logique exacte du script R"""
        self.logger.info("Construction BD_TVA complète (reproduction R)")

        start_time = time.time()
        if self.IS_LOCAL_MODE:
            bd_tva = pd.read_csv("data/bd_tva_complete.csv")
        else:
            bd_tva = pd.read_sql(sql_tva_complete, connection)
            bd_tva.to_csv("data/bd_tva_complete.csv", index=False, encoding="utf-8")
        bd_tva.columns = bd_tva.columns.str.upper()

        elapsed_time = time.time() - start_time
        self.logger.info(
            f"BD_TVA construite (R-compatible): {len(bd_tva)} lignes en {elapsed_time:.2f}s"
        )
        return bd_tva

    def _extract_dcf_tva_deduction(self, connection) -> pd.DataFrame:
        """Extraction des déductions TVA détaillées"""
        self.logger.info("Extraction DCF_TVA_DEDUCTION")

        try:
            start_time = time.time()
            if self.IS_LOCAL_MODE:
                deductions = pd.read_csv("data/dcf_tva_deduction.csv")
            else:
                deductions = pd.read_sql(sql_tva_deduction, connection)
                deductions.to_csv(
                    "data/dcf_tva_deduction.csv", index=False, encoding="utf-8"
                )
            deductions.columns = deductions.columns.str.upper()

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"DCF_TVA_DEDUCTION extraite: {len(deductions)} lignes en {elapsed_time:.2f}s"
            )
            return deductions

        except Exception as e:
            self.logger.warning(f"Erreur extraction DCF_TVA_DEDUCTION: {e}")
            return pd.DataFrame()

    def _extract_dgd_complete_with_nomenclature(self, connection) -> pd.DataFrame:
        """Extraction complète des données DGD avec nomenclature SH"""
        self.logger.info("Extraction DGD complète avec nomenclature")

        try:
            start_time = time.time()
            if self.IS_LOCAL_MODE:
                dgd_data = pd.read_csv("data/dgd_complete.csv")
            else:
                dgd_data = pd.read_sql(sql_dgd, connection)
                dgd_data.to_csv("data/dgd_complete.csv", index=False, encoding="utf-8")
            dgd_data.columns = dgd_data.columns.str.upper()

            dgd_data["ANNEE"] = dgd_data["DATE_LIQUIDATION"].astype(str).str[:4]
            dgd_data["TVA"] = dgd_data["TVA"].fillna(0)

            if "NOMENCLATURE10" in dgd_data.columns:
                dgd_data["CODE_CHAPITRE"] = (
                    dgd_data["NOMENCLATURE10"].astype(str).str[:2]
                )
                if not self.nomenclature_sh.empty:
                    try:
                        nomenclature = self.nomenclature_sh.copy()
                        if "Code_Chapitre" in nomenclature.columns:
                            nomenclature["Code_Chapitre"] = nomenclature[
                                "Code_Chapitre"
                            ].apply(
                                lambda x: str(int(x))
                                if pd.notna(x) and int(x) > 9
                                else f"0{int(x)}"
                                if pd.notna(x)
                                else ""
                            )
                            dgd_data = dgd_data.merge(
                                nomenclature,
                                left_on="CODE_CHAPITRE",
                                right_on="Code_Chapitre",
                                how="left",
                            )
                            self.logger.info("Nomenclature fusionnée sur Code_Chapitre")
                    except Exception as e:
                        self.logger.warning(
                            f"Erreur lors de la fusion nomenclature: {e}"
                        )

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"DGD complète extraite: {len(dgd_data)} lignes en {elapsed_time:.2f}s"
            )
            return dgd_data

        except Exception as e:
            self.logger.error(f"Erreur lors de l'extraction DGD: {e}")
            return pd.DataFrame()

    def _extract_programmation_controles(self, connection) -> pd.DataFrame:
        """Extraction des données de programmation et contrôles"""
        self.logger.info("Extraction programmation et contrôles")

        try:
            start_time = time.time()
            if self.IS_LOCAL_MODE:
                programmation = pd.read_csv("data/programmation_controles.csv")
            else:
                programmation = pd.read_sql(sql_programmations_control, connection)
                programmation.to_csv(
                    "data/programmation_controles.csv", index=False, encoding="utf-8"
                )
            programmation.columns = programmation.columns.str.upper()

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"Programmation extraite: {len(programmation)} lignes en {elapsed_time:.2f}s"
            )
            return programmation

        except Exception as e:
            self.logger.warning(f"Erreur extraction programmation: {e}")
            return pd.DataFrame()

    def _extract_benefices_complete(self, connection) -> pd.DataFrame:
        """Extraction complète des bénéfices (IBNC, IBICA, IS)"""
        self.logger.info("Extraction bénéfices complets")

        try:
            start_time = time.time()
            if self.IS_LOCAL_MODE:
                benefices = pd.read_csv("data/benefices_complete.csv")
            else:
                benefices = pd.read_sql(sql_benefices_complete, connection)
                benefices.to_csv(
                    "data/benefices_complete.csv", index=False, encoding="utf-8"
                )
            benefices.columns = benefices.columns.str.upper()

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"Bénéfices extraits: {len(benefices)} lignes en {elapsed_time:.2f}s"
            )
            return benefices

        except Exception as e:
            self.logger.warning(f"Erreur extraction bénéfices: {e}")
            return pd.DataFrame()

    def merge_all_data(self, extracted_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Fusion de toutes les données extraites"""
        self.logger.info("=== FUSION DES DONNÉES ===")

        # Commencer par les contribuables comme base
        merged_data = extracted_data["contribuables"].copy()

        # Fusionner progressivement chaque source de données
        if not extracted_data["bd_tva"].empty:
            merged_data = merged_data.merge(
                extracted_data["bd_tva"], on="NUM_IFU", how="left"
            )

        if not extracted_data["dgd_data"].empty:
            # Agrégation des données DGD par IFU selon la logique R
            dgd_aggregated = self._aggregate_dgd_data(extracted_data["dgd_data"])
            merged_data = merged_data.merge(
                dgd_aggregated, left_on="NUM_IFU", right_on="NUM_IFU", how="left"
            )

        if not extracted_data["benefices"].empty:
            merged_data = merged_data.merge(
                extracted_data["benefices"], on="NUM_IFU", how="left"
            )

        if not extracted_data["programmation"].empty:
            merged_data = merged_data.merge(
                extracted_data["programmation"], on="NUM_IFU", how="left"
            )

        self.logger.info(
            f"Données fusionnées: {len(merged_data)} lignes, {len(merged_data.columns)} colonnes"
        )
        merged_data.to_csv("data/merged_data.csv", index=False, encoding="utf-8")

        return merged_data

    def _aggregate_dgd_data(self, dgd_data: pd.DataFrame) -> pd.DataFrame:
        """Agrégation des données DGD par IFU selon la logique R"""

        # Vérifier si nous avons des données et les colonnes nécessaires
        required_columns = ["IFU", "DATE_LIQUIDATION", "FLUX", "CAF", "FOB", "TVA"]
        available_columns = [col for col in required_columns if col in dgd_data.columns]

        if dgd_data.empty or len(available_columns) < 3:
            self.logger.warning(
                f"Données DGD insuffisantes. Colonnes disponibles: {available_columns}"
            )
            # Retourner un DataFrame vide avec les colonnes attendues
            return pd.DataFrame(
                columns=[
                    "NUM_IFU",
                    "ANNEE",
                    "IMPORT_CAF",
                    "IMPORT_FOB",
                    "IMPORT_TVA",
                    "EXPORT_CAF",
                    "EXPORT_FOB",
                    "EXPORT_TVA",
                ]
            )

        try:
            # Reproduire la logique R : DGD$ANNEE=substr(DGD$DATE_LIQUIDATION,1,4)
            if "DATE_LIQUIDATION" in dgd_data.columns:
                dgd_data["ANNEE"] = dgd_data["DATE_LIQUIDATION"].astype(str).str[:4]

            # Préparer les colonnes avec des valeurs par défaut
            for col in ["CAF", "FOB", "TVA"]:
                if col not in dgd_data.columns:
                    dgd_data[col] = 0

            # Reproduire : DGD_AN=aggregate(cbind(CAF, FOB, TVA)~IFU+FLUX+ANNEE,DGD,sum)
            dgd_an = (
                dgd_data.groupby(["IFU", "FLUX", "ANNEE"])
                .agg({"CAF": "sum", "FOB": "sum", "TVA": "sum"})
                .reset_index()
            )

            # Reproduire : DGD_AN_IMPORT=subset(DGD_AN,DGD_AN$FLUX=="I")
            dgd_an_import = dgd_an[dgd_an["FLUX"] == "I"].copy()
            if not dgd_an_import.empty:
                dgd_an_import.columns = [
                    "NUM_IFU",
                    "FLUX",
                    "ANNEE",
                    "IMPORT_CAF",
                    "IMPORT_FOB",
                    "IMPORT_TVA",
                ]
                dgd_an_import = dgd_an_import[
                    ["NUM_IFU", "ANNEE", "IMPORT_CAF", "IMPORT_FOB", "IMPORT_TVA"]
                ]
            else:
                dgd_an_import = pd.DataFrame(
                    columns=[
                        "NUM_IFU",
                        "ANNEE",
                        "IMPORT_CAF",
                        "IMPORT_FOB",
                        "IMPORT_TVA",
                    ]
                )

            # Reproduire : DGD_AN_EXPORT=subset(DGD_AN,DGD_AN$FLUX=="E")
            dgd_an_export = dgd_an[dgd_an["FLUX"] == "E"].copy()
            if not dgd_an_export.empty:
                dgd_an_export.columns = [
                    "NUM_IFU",
                    "FLUX",
                    "ANNEE",
                    "EXPORT_CAF",
                    "EXPORT_FOB",
                    "EXPORT_TVA",
                ]
                dgd_an_export = dgd_an_export[
                    ["NUM_IFU", "ANNEE", "EXPORT_CAF", "EXPORT_FOB", "EXPORT_TVA"]
                ]
            else:
                dgd_an_export = pd.DataFrame(
                    columns=[
                        "NUM_IFU",
                        "ANNEE",
                        "EXPORT_CAF",
                        "EXPORT_FOB",
                        "EXPORT_TVA",
                    ]
                )

            # Reproduire : DGD_IMPORT_EXPORT=full_join(DGD_AN_IMPORT,DGD_AN_EXPORT,by=c("NUM_IFU","ANNEE"))
            if not dgd_an_import.empty or not dgd_an_export.empty:
                dgd_import_export = pd.merge(
                    dgd_an_import, dgd_an_export, on=["NUM_IFU", "ANNEE"], how="outer"
                )
                dgd_import_export = dgd_import_export.fillna(0)
            else:
                dgd_import_export = pd.DataFrame(
                    columns=[
                        "NUM_IFU",
                        "ANNEE",
                        "IMPORT_CAF",
                        "IMPORT_FOB",
                        "IMPORT_TVA",
                        "EXPORT_CAF",
                        "EXPORT_FOB",
                        "EXPORT_TVA",
                    ]
                )

            return dgd_import_export

        except Exception as e:
            self.logger.error(f"Erreur lors de l'agrégation DGD: {e}")
            return pd.DataFrame(
                columns=[
                    "NUM_IFU",
                    "ANNEE",
                    "IMPORT_CAF",
                    "IMPORT_FOB",
                    "IMPORT_TVA",
                    "EXPORT_CAF",
                    "EXPORT_FOB",
                    "EXPORT_TVA",
                ]
            )

    def get_final_file_structure(self) -> pd.DataFrame:
        """
        Retourne la structure finale du fichier de résultats
        """
        columns = [
            "NUM_IFU",
            "NOM_IFU",
        ]
        df = pd.DataFrame(columns=columns)
        return df

    def calculate_all_indicators(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calcul de tous les indicateurs de risque en utilisant les classes modulaires
        """
        self.logger.info("=== CALCUL DE TOUS LES INDICATEURS ===")

        df = self.get_final_file_structure()

        # Indicateurs TVA (1, 2, 8, 12, 13, 14)
        merged_data = TVAIndicators.calculate_indicator_1(merged_data)
        merged_data = TVAIndicators.calculate_indicator_2(merged_data)
        merged_data = TVAIndicators.calculate_indicator_8(merged_data)
        merged_data = TVAIndicators.calculate_indicator_12(merged_data)
        merged_data = TVAIndicators.calculate_indicator_13(merged_data)
        merged_data = TVAIndicators.calculate_indicator_14(merged_data)

        # Indicateurs Import/Export (3, 4, 5)
        merged_data = ImportExportIndicators.calculate_indicator_3(merged_data)
        merged_data = ImportExportIndicators.calculate_indicator_4(merged_data)
        merged_data = ImportExportIndicators.calculate_indicator_5(merged_data)

        # Indicateurs Comptabilité (20, 21, 23, 27, 29)
        merged_data = ComptabiliteIndicators.calculate_indicator_20(merged_data)
        merged_data = ComptabiliteIndicators.calculate_indicator_21(merged_data)
        merged_data = ComptabiliteIndicators.calculate_indicator_23(merged_data)
        merged_data = ComptabiliteIndicators.calculate_indicator_27(merged_data)
        merged_data = ComptabiliteIndicators.calculate_indicator_29(merged_data)

        # Indicateurs Contrôle (15, 16)
        merged_data = ControleIndicators.calculate_indicator_15(merged_data)
        merged_data = ControleIndicators.calculate_indicator_16(merged_data)

        # Indicateurs Avancés (37, 38, 39, 46, 47, 49, 57, 58)
        merged_data = AdvancedIndicators.calculate_indicator_37(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_38(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_39(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_46(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_47(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_49(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_57(merged_data)
        merged_data = AdvancedIndicators.calculate_indicator_58(merged_data)

        self.logger.info("Calcul de tous les indicateurs terminé")
        return merged_data

    def generate_risk_summary(self, results_data: pd.DataFrame) -> Dict:
        """Génération du résumé des risques"""
        self.logger.info("=== GÉNÉRATION DU RÉSUMÉ DES RISQUES ===")

        # Colonnes d'indicateurs de risque
        risk_columns = [
            col for col in results_data.columns if col.startswith("RISQUE_IND_")
        ]

        summary = {
            "total_entreprises": len(results_data),
            "indicateurs_calcules": len(risk_columns),
            "repartition_risques": {},
            "top_risques": {},
            "timestamp": datetime.now().isoformat(),
        }

        # Analyser chaque indicateur
        for col in risk_columns:
            risk_counts = results_data[col].value_counts()
            summary["repartition_risques"][col] = risk_counts.to_dict()

        # Identifier les entreprises à haut risque
        rouge_columns = [
            col for col in risk_columns if "rouge" in results_data[col].values
        ]
        if rouge_columns:
            high_risk_enterprises = results_data[
                results_data[rouge_columns].eq("rouge").any(axis=1)
            ]["NUM_IFU"].tolist()
            summary["entreprises_haut_risque"] = high_risk_enterprises[:50]  # Top 50

        self.logger.info(
            f"Résumé généré: {summary['total_entreprises']} entreprises, {summary['indicateurs_calcules']} indicateurs"
        )
        return summary

    def export_results(
        self, results_data: pd.DataFrame, summary: Dict, base_filename: str = None
    ):
        """Export des résultats vers différents formats"""
        if base_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"risk_analysis_{timestamp}"

        self.logger.info("=== EXPORT DES RÉSULTATS ===")

        try:
            # Export CSV
            csv_path = f"output/{base_filename}.csv"
            results_data.to_csv(csv_path, index=False, encoding="utf-8")
            self.logger.info(f"Export CSV: {csv_path}")

            # Export Excel
            excel_path = f"output/{base_filename}.xlsx"
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                results_data.to_excel(writer, sheet_name="Resultats", index=False)

                # Ajouter des feuilles de résumé si possible
                if summary:
                    summary_df = pd.DataFrame([summary])
                    summary_df.to_excel(writer, sheet_name="Resumé", index=False)

            self.logger.info(f"Export Excel: {excel_path}")

            # Export JSON du résumé
            if summary:
                import json

                json_path = f"output/{base_filename}_summary.json"
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
                self.logger.info(f"Export JSON: {json_path}")

        except Exception as e:
            self.logger.error(f"Erreur lors de l'export: {e}")

    def run_complete_analysis(self, export_results: bool = True) -> Dict[str, any]:
        """
        Exécution complète de l'analyse de risque
        Compatible avec l'interface de l'application Dash
        """
        self.logger.info("=== DÉMARRAGE ANALYSE COMPLÈTE (MODULAIRE) ===")
        start_time = time.time()

        try:
            # 1. Extraction des données
            extracted_data = self.extract_complete_oracle_data()

            # 2. Fusion des données
            if self.IS_LOCAL_MODE and os.path.exists("data/merged_data.csv"):
                merged_data = pd.read_csv("data/merged_data.csv")
            else:
                merged_data = self.merge_all_data(extracted_data)

            # 3. Calcul des indicateurs
            results_data = self.calculate_all_indicators(merged_data)

            # 4. Génération du résumé
            summary = self.generate_risk_summary(results_data)

            # 5. Export des résultats
            nb_reports = 0
            if export_results:
                self.export_results(results_data, summary)
                nb_reports = 3  # CSV, Excel, Summary JSON

            elapsed_time = time.time() - start_time
            nb_contribuables = len(results_data)

            self.logger.info(f"=== ANALYSE TERMINÉE EN {elapsed_time:.2f}s ===")
            self.logger.info(f"Contribuables analysés: {nb_contribuables}")
            self.logger.info(f"Rapports générés: {nb_reports}")

            # Retourner le format attendu par l'app Dash
            return {
                "success": True,
                "nb_contribuables": nb_contribuables,
                "nb_reports": nb_reports,
                "duree_analyse": f"{elapsed_time:.2f}s",
                "summary": summary,
                "data": results_data,  # Optionnel pour debug
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
