import pandas as pd
import logging
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows 
import xlsxwriter
import os
import networkx as nx
import matplotlib.pyplot as plt 
from matplotlib import animation
from datetime import date
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        #logging.FileHandler(log_file_path, mode="a"),
    ],
)
class FichesGenerator:
    """ 
    quantume_name: le nom du quantume pour lequel on génère les fiches
    quantume: le dataframe contenant les contribuables du quantume
    ifu_data: le dataframe contenant les données IFU
    risk_data: le dataframe contenant les données de risque
    data_dgd: le dataframe contenant les données douanières

    """
    OUTPUT_DIR = "../output"
    DATA_DIR = "../data"
    DOCS_DIR = "../docs"
    def __init__(self,
                 quantume_name:str,
                quantume:pd.DataFrame ,
                ifu_data:pd.DataFrame,
                risk_data:pd.DataFrame,
                data_dgd:pd.DataFrame):
        self.logger = logging.getLogger(__name__)
        self.quantume_name = quantume_name
        self.quantume = quantume
        self.ifu_data = ifu_data
        self.risk_data = risk_data
        self.data_dgd = data_dgd 
        self.load_reference_data()

    def load_reference_data(self):
        """Charger tous les fichiers de référence utilisés par les scripts R"""
        self.logger.info("Chargement des données de référence...")

        try:
            # 1. Liste des non-éligibles (NON_EIGIBLE.xlsx)
            reference_files = {
            "meta_donnees": f"{self.DOCS_DIR}/META_DONNEES.xlsx",
            "fiche_pistes": f"{self.DOCS_DIR}/FICHE_PISTES_INVESTIGATION.xlsx",
            "nomenclature_sh": f"{self.DOCS_DIR}/NomenclatureSH.xlsx",
            "dcf_activites": f"{self.DOCS_DIR}/DCF_ACTIVITES.xlsx",
            "suivi": f"{self.DOCS_DIR}/SUIVI.xlsx"
            }

            for attr_name, file_path in reference_files.items():
                if os.path.exists(file_path):
                    setattr(self, attr_name, pd.read_excel(file_path))
                    self.logger.info(f"{file_path} chargé")
                else:
                    setattr(self, attr_name, pd.DataFrame())
                    self.logger.warning(f"{file_path} non trouvé")

        except Exception as e:
            self.logger.error(
                f"Erreur lors du chargement des données de référence: {e}"
            )

    def process(self):
        # Placeholder for processing logic
        self.ifu_data.columns = self.ifu_data.columns.str.upper()
        self.quantume.columns = self.quantume.columns.str.upper()
        self.risk_data.columns = self.risk_data.columns.str.upper()
        data = self.quantume.merge(self.risk_data,
                                on='NUM_IFU',
                                how='left').merge(self.ifu_data,
                                            on='NUM_IFU',
                                            how='inner')
        data["SOUS_UR"] = data["STRUCTURES_y"].str.replace(r'[ /]', '_', regex=True)
        data["UR"] = data["STRUCTURES_x"].str.replace(r'[ /]', '_', regex=True)
        data["BRIGADES"] = data["BRIGADES"].str.replace(r'[ /]', '_', regex=True)
        data["contribuables"] = data.apply(lambda row:f"{row['NUM_IFU']}_{row['SOUS_UR']}_{row['BRIGADES']}", axis=1)
        data["contribuables"] = data["contribuables"].str.replace(r'[^a-zA-Z0-9]', '_', regex=True)
        #traitement des dates
        data["DATE_DERNIERE_AVIS"] = pd.to_datetime(data["DATE_DERNIERE_AVIS"], errors='coerce')

        return data

    def generate(self,programme_root:str="../fiches"):
        """
        1 .cree dans le repertoire  programme_root un sous dossiers du nom self.quantume_name
        2. pour chaque contribuable de self.quantume,
          -cree un repertoire du nom de l'UR/SOUS_UR/BV
          -cree une fiche excel dans le repertoire correspondant
          -le fichier excel doit comporter deux feuilles : 
             Fiche_Contribuables et Fiche_Analyse_Risque,FICHE_PISTES_INVESTIGATION,META_DONNEES,
             NomenclatureSH,Donnees_douanieres,Client_Detail_TVA,Fournisseur_Detail_TVA,Reseau_Client,Reseau_Fourn
        -la feuille Fiche_Contribuables doit comporter les informations suivantes :tous les informations du contribuable provenant de ifu_data
        -la feuille Fiche_Analyse_Risque doit comporter les informations suivantes :tous les informations du contribuable provenant de risk_data
        il faut noter que chaque contribuable ifu peut avoir plusieurs lignes dans risk_data,le ifu et la colonne annee sont les clés 
        -la feillle FICHE_PISTES_INVESTIGATION contient tous les infos provenant de fiche_pistes_investigation.xlsx
        -la feillle META_DONNEES contient tous les infos provenant de META_DONNEES.xlsx
        -la feillle NomenclatureSH contient tous les infos provenant de NomenclatureSH.xlsx
        -la feillle Donnees_douanieres contient tous les infos provenant de data_dgd.xlsx pour le contribuable il y'a egalement la colonne ifu et annee
        
        
        """
        data = self.process()
        
        # 1. Créer le répertoire principal du programme
        programme_path = os.path.join(programme_root, self.quantume_name)
        os.makedirs(programme_path, exist_ok=True)
        self.logger.info(f"Répertoire de programme créé: {programme_path}")
        
        # Compteurs pour le suivi
        fiches_generees = 0
        erreurs = 0
        
        # 2. Pour chaque contribuable
        for index, row in data.iterrows():
            try:
                num_ifu = row['NUM_IFU']
                ur = row['UR']
                sous_ur = row['SOUS_UR']
                brigades = row['BRIGADES']
                contribuable_name = row['contribuables']
                
                # Créer la structure de répertoires UR/SOUS_UR/BRIGADES
                fiche_dir = os.path.join(programme_path, ur, sous_ur, brigades)
                os.makedirs(fiche_dir, exist_ok=True)
                
                # Nom du fichier Excel
                excel_filename = f"{contribuable_name}.xlsx"
                excel_path = os.path.join(fiche_dir, excel_filename)
                
                # Créer le fichier Excel avec xlsxwriter
                with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                    
                    # Feuille 1: Fiche_Contribuables (toutes les infos de ifu_data)
                    contribuable_info = self.ifu_data[self.ifu_data['NUM_IFU'] == num_ifu]
                    if not contribuable_info.empty:
                        contribuable_info.to_excel(writer, sheet_name='Fiche_Contribuables', index=False)
                    
                    # Feuille 2: Fiche_Analyse_Risque (toutes les lignes de risk_data pour cet IFU)
                    risk_info = self.risk_data[self.risk_data['NUM_IFU'] == num_ifu]
                    if not risk_info.empty:
                        risk_info.to_excel(writer, sheet_name='Fiche_Analyse_Risque', index=False)
                    
                    # Feuille 3: FICHE_PISTES_INVESTIGATION
                    if not self.fiche_pistes.empty:
                        self.fiche_pistes.to_excel(writer, sheet_name='FICHE_PISTES_INVEST', index=False)
                    
                    # Feuille 4: META_DONNEES
                    if not self.meta_donnees.empty:
                        self.meta_donnees.to_excel(writer, sheet_name='META_DONNEES', index=False)
                    
                    # Feuille 5: NomenclatureSH
                    if not self.nomenclature_sh.empty:
                        self.nomenclature_sh.to_excel(writer, sheet_name='NomenclatureSH', index=False)
                    
                    # Feuille 6: Donnees_douanieres (filtrées par IFU)
                    if not self.data_dgd.empty:
                        # Vérifier si la colonne NUM_IFU existe dans data_dgd
                        if 'NUM_IFU' in self.data_dgd.columns:
                            douanes_info = self.data_dgd[self.data_dgd['NUM_IFU'] == num_ifu]
                        elif 'IFU' in self.data_dgd.columns:
                            douanes_info = self.data_dgd[self.data_dgd['IFU'] == num_ifu]
                        else:
                            douanes_info = pd.DataFrame()
                        
                        if not douanes_info.empty:
                            douanes_info.to_excel(writer, sheet_name='Donnees_douanieres', index=False)
                    
                    # Feuilles supplémentaires (Client_Detail_TVA, Fournisseur_Detail_TVA, etc.)
                    # Ces feuilles peuvent être ajoutées si les données sont disponibles
                    # Pour l'instant, créer des feuilles vides ou des placeholders
                    
                    # Placeholder pour les autres feuilles mentionnées
                    pd.DataFrame().to_excel(writer, sheet_name='Client_Detail_TVA', index=False)
                    pd.DataFrame().to_excel(writer, sheet_name='Fournisseur_Detail_TVA', index=False)
                    pd.DataFrame().to_excel(writer, sheet_name='Reseau_Client', index=False)
                    pd.DataFrame().to_excel(writer, sheet_name='Reseau_Fourn', index=False)
                
                fiches_generees += 1
                self.logger.info(f"Fiche générée pour {num_ifu}: {excel_path}")
                
            except Exception as e:
                erreurs += 1
                self.logger.error(f"Erreur lors de la génération de la fiche pour {row.get('NUM_IFU', 'IFU inconnu')}: {e}")
        
        # Message de résumé
        summary = f"Génération terminée: {fiches_generees} fiches créées, {erreurs} erreurs"
        self.logger.info(summary)
        
        return f"Generated programme: {programme_path} - {summary}"