import os
import glob
import re
from pathlib import Path


def get_latest_risk_file(directory: str = "../data/risk_contribuables") -> str | None:
    """
    Recherche et retourne le fichier RISK_INDICATEUR_CONTRIBUABLES le plus récent
    dans le répertoire spécifié, basé sur la date dans le nom du fichier.
    
    Args:
        directory: Chemin du répertoire à parcourir (par défaut "../data")
        
    Returns:
        Chemin complet du fichier le plus récent, ou None si aucun fichier trouvé
        
    Example:
        Si le répertoire contient:
        - RISK_INDICATEUR_CONTRIBUABLES_20251217.csv
        - RISK_INDICATEUR_CONTRIBUABLES_20251218.csv
        - RISK_INDICATEUR_CONTRIBUABLES_20251219.csv
        
        La fonction retournera RISK_INDICATEUR_CONTRIBUABLES_20251219.csv
    """
    pattern = os.path.join(directory, "RISK_INDICATEUR_CONTRIBUABLES_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Extraire la date du nom de fichier et trier
    def extract_date(filepath: str) -> str:
        filename = os.path.basename(filepath)
        match = re.search(r'RISK_INDICATEUR_CONTRIBUABLES_(\d{8})\.csv', filename)
        return match.group(1) if match else "00000000"
    
    # Trier par la date dans le nom du fichier (le plus récent en premier)
    latest_file = max(files, key=extract_date)
    return os.path.basename(latest_file)


BASE_COLUMNS = [
    "NUM_IFU",
    "ANNEE",
    "CODE_STRUCTURE",
    "LIBELLE_STRUCTURE",
    "RAISON_SOCIALE",
    "PERIODE_FISCALE",
    "ETAT",
    "REGIME_FISCALE",
    "DATE_DEBUT_ACTIVITE",
    "SECTEUR_ACTIVITE",
    "TYPE_CONTROLE",
    "FORME_JURIDIQUE",
    "HIERARCHIE_2",
    "HIERARCHIE_3",
    "HIERARCHIE_2",
    "HIERARCHIE_3",
    "RISQUE_IND_1",
    "GAP_IND_1",
    "SCORE_IND_1",
    "RISQUE_IND_2",
    "GAP_IND_2",
    "SCORE_IND_2",
    "RISQUE_IND_12",
    "GAP_IND_12",
    "SCORE_IND_12",
    "RISQUE_IND_8",
    "GAP_IND_8",
    "SCORE_IND_8",
    "RISQUE_IND_14",
    "RISQUE_IND_13",
    "GAP_IND_13",
    "SCORE_IND_13",
    "RISQUE_IND_3",
    "AGE_MOIS_IND_3",
    "RISQUE_IND_4",
    "GAP_IND_4",
    "SCORE_IND_4",
    "RISQUE_IND_5",
    "GAP_IND_5",
    "SCORE_IND_5",
    "RISQUE_IND_20",
    "GAP_IND_20",
    "SCORE_IND_20",
    "RISQUE_IND_27",
    "RISQUE_IND_15_A",
    "RISQUE_IND_15_B",
    "RISQUE_IND_16",
    "RISQUE_IND_38",
    "RISQUE_IND_39",
    "RISQUE_IND_46",
    "RISQUE_IND_47",
    "RISQUE_IND_49",
    "RISQUE_IND_57",
    "RISQUE_IND_58",
]

LABEL_COLUMNS = {
    "NUM_IFU": "Numéro IFU",
    "ANNEE": "Année",
    "CODE_STRUCTURE": "Code Structure",
    "LIBELLE_STRUCTURE": "Libellé Structure",
    "RAISON_SOCIALE": "Raison Sociale",
    "PERIODE_FISCALE": "Période Fiscale",
    "ETAT": "État",
    "REGIME_FISCALE": "Régime Fiscal",
    "DATE_DEBUT_ACTIVITE": "Date Début Activité",
    "SECTEUR_ACTIVITE": "Secteur d'Activité",
    "TYPE_CONTROLE": "Type de Contrôle",
    "FORME_JURIDIQUE": "Forme Juridique",
    "HIERARCHIE_2": "Hiérarchie 2",
    "HIERARCHIE_3": "Hiérarchie 3",
    # indicateur 1
    "RISQUE_IND_1": "Indicateur 1",
    "GAP_IND_1": "Écart Indicateur 1",
    "SCORE_IND_1": "Score Indicateur 1",
    # indicateur 2
    "RISQUE_IND_2": "Indicateur 2",
    "GAP_IND_2": "Écart Indicateur 2",
    "SCORE_IND_2": "Score Indicateur 2",
    # indicateur 3
    "RISQUE_IND_3": "Indicateur 3",
    "GAP_IND_3": "Écart Indicateur 3",
    "AGE_MOIS_IND_3": "Âge en Mois Indicateur  3",
    # indicateur 4
    "RISQUE_IND_4": "Indicateur 4",
    "GAP_IND_4": "Écart Indicateur 4",
    "SCORE_IND_4": "Score Indicateur 4",
    # indicateur 5
    "RISQUE_IND_5": "Indicateur 5",
    "GAP_IND_5": "Écart Indicateur 5",
    "SCORE_IND_5": "Score Indicateur 5",
    # indicateur 12
    "RISQUE_IND_12": "Indicateur 12",
    "GAP_IND_12": "Écart Indicateur 12",
    "SCORE_IND_12": "Score Indicateur 12",
    # indicateur 8
    "RISQUE_IND_8": "Indicateur 8",
    "GAP_IND_8": "Écart Indicateur 8",
    "SCORE_IND_8": "Score Indicateur 8",
    # indicateur 13
    "RISQUE_IND_13": "Indicateur 13",
    "GAP_IND_13": "Écart Indicateur 13",
    "SCORE_IND_13": "Score Indicateur 13",
    # indicateur 14
    "RISQUE_IND_14": "Indicateur 14",
    "GAP_IND_14": "Écart Indicateur 14",
    "SCORE_IND_14": "Score Indicateur 14",
    # indicateur 20
    "RISQUE_IND_20": "Indicateur 20",
    "GAP_IND_20": "Écart Indicateur 20",
    "SCORE_IND_20": "Score Indicateur 20",
    # indicateur 27
    "RISQUE_IND_27": "Indicateur 27",
    "RISQUE_IND_15_A": "Indicateur 15A",
    "RISQUE_IND_15_B": "Indicateur 15B",
    "RISQUE_IND_16": "Indicateur 16",
    "RISQUE_IND_38": "Indicateur 38",
    "RISQUE_IND_39": "Indicateur 39",
    "RISQUE_IND_46": "Indicateur 46",
    "RISQUE_IND_47": "Indicateur 47",
    "RISQUE_IND_49": "Indicateur 49",
    "Actions": "Actions",
}
