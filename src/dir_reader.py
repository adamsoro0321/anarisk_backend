import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class FileInfo:
    """Représente les informations d'un fichier"""
    name: str
    path: str
    extension: str
    size: int
    modified_date: str
    file_type: str  # 'xlsx', 'png_chart', 'png_forecast', 'other'
    ifu: Optional[str] = None  # IFU extrait du nom de fichier


@dataclass
class FolderInfo:
    """Représente les informations d'un dossier"""
    name: str
    path: str
    files_count: int
    subfolders_count: int
    subfolders: List[str]


class DirReader:
    """
    Classe pour explorer et lire le dossier fiches et ses sous-dossiers.
    
    Structure hiérarchique (4 niveaux):
    fiches/
        programme_YYYY_MM_DD/           <- Programme (Niveau 0)
            STRUCTURE/                   <- Structure (Niveau 1) ex: DGE, DME_CI, DRI_CN
                SOUS_STRUCTURE/          <- Sous-structure (Niveau 2) ex: DIRECTION_GRANDES ENTREPRISES, DCI_OUAGA_1
                    BRIGADE/             <- Brigade (Niveau 3) ex: BV1_DGE, BV3_DME-CI
                        fichiers (.xlsx, .png)
    
    Exemples de chemins complets:
    - programme_2025_12_25/DGE/DIRECTION_GRANDES ENTREPRISES/BV1_DGE
    - programme_2025_12_25/DME_CI/DCI_OUAGA_1/BV3_DME-CI
    """
    
    def __init__(self, root_path: str = None):
        """
        Initialise le DirReader avec le chemin racine du dossier programmes.
        
        Args:
            root_path: Chemin vers le dossier programmes. 
                      Si None, utilise le chemin relatif par défaut.
        """
        if root_path is None:
            # Chemin par défaut relatif au fichier actuel
            current_dir = Path(__file__).parent.parent
            self.root_dir = current_dir / "fiches"
        else:
            self.root_dir = Path(root_path)
        
        if not self.root_dir.exists():
            raise FileNotFoundError(f"Le dossier fiches n'existe pas: {self.root_dir}")
    
    # ========== FONCTIONS DE LISTING - NIVEAU 0: PROGRAMMES ==========
    
    def list_programmes(self) -> List[Dict[str, Any]]:
        """
        Liste tous les programmes disponibles (dossiers de premier niveau).
        
        Returns:
            Liste des programmes avec leurs informations (nom, date, chemin, nombre de structures)
        """
        programmes = []
        for item in self.root_dir.iterdir():
            if item.is_dir() and item.name.startswith("programme_"):
                # Extraire la date du nom du programme (format: programme_YYYY_MM_DD)
                try:
                    date_str = item.name.replace("programme_", "")
                    date_parts = date_str.split("_")
                    date_formatted = f"{date_parts[2]}/{date_parts[1]}/{date_parts[0]}"
                except (IndexError, ValueError):
                    date_formatted = "Date inconnue"
                
                structures_count = len([d for d in item.iterdir() if d.is_dir()])
                
                programmes.append({
                    "name": item.name,
                    "path": str(item),
                    "date": date_formatted,
                    "structures_count": structures_count
                })
        
        # Trier par date décroissante
        programmes.sort(key=lambda x: x["name"], reverse=True)
        return programmes
    
    # ========== FONCTIONS DE LISTING - NIVEAU 1: STRUCTURES ==========
    
    def list_structures(self, programme_name: str) -> List[Dict[str, Any]]:
        """
        Liste toutes les structures dans un programme donné.
        
        Args:
            programme_name: Nom du programme (ex: 'programme_2025_12_25')
            
        Returns:
            Liste des structures avec leurs informations (code, sous-structures, fichiers)
        """
        programme_path = self.root_dir / programme_name
        if not programme_path.exists():
            raise FileNotFoundError(f"Programme non trouvé: {programme_name}")
        
        structures = []
        for item in programme_path.iterdir():
            if item.is_dir():
                # Compter les sous-structures (dossiers directs)
                sous_structures = [d for d in item.iterdir() if d.is_dir()]
                # Compter tous les fichiers récursivement
                files = [f for f in item.rglob("*") if f.is_file()]
                xlsx_files = [f for f in files if f.suffix.lower() == '.xlsx']
                
                structures.append({
                    "code": item.name,
                    "path": str(item),
                    "sous_structures_count": len(sous_structures),
                    "total_files": len(files),
                    "total_contribuables": len(xlsx_files)
                })
        
        structures.sort(key=lambda x: x["code"])
        return structures
    
    # ========== FONCTIONS DE LISTING - NIVEAU 2: SOUS-STRUCTURES ==========
    
    def list_sous_structures(self, programme_name: str, structure_code: str) -> List[Dict[str, Any]]:
        """
        Liste toutes les sous-structures dans une structure donnée.
        
        Args:
            programme_name: Nom du programme (ex: 'programme_2025_12_25')
            structure_code: Code de la structure (ex: 'DGE', 'DME_CI')
            
        Returns:
            Liste des sous-structures avec leurs informations
        """
        structure_path = self.root_dir / programme_name / structure_code
        if not structure_path.exists():
            raise FileNotFoundError(f"Structure non trouvée: {structure_code}")
        
        sous_structures = []
        for item in structure_path.iterdir():
            if item.is_dir():
                # Compter les brigades (dossiers directs)
                brigades = [d for d in item.iterdir() if d.is_dir()]
                # Compter tous les fichiers récursivement
                files = [f for f in item.rglob("*") if f.is_file()]
                xlsx_files = [f for f in files if f.suffix.lower() == '.xlsx']
                
                sous_structures.append({
                    "name": item.name,
                    "path": str(item),
                    "brigades_count": len(brigades),
                    "total_files": len(files),
                    "total_contribuables": len(xlsx_files)
                })
        
        sous_structures.sort(key=lambda x: x["name"])
        return sous_structures
    
    # ========== FONCTIONS DE LISTING - NIVEAU 3: BRIGADES ==========
    
    def list_brigades(self, programme_name: str, structure_code: str, 
                      sous_structure_name: str) -> List[Dict[str, Any]]:
        """
        Liste toutes les brigades dans une sous-structure donnée.
        
        Args:
            programme_name: Nom du programme
            structure_code: Code de la structure (ex: 'DGE', 'DME_CI')
            sous_structure_name: Nom de la sous-structure (ex: 'DIRECTION_GRANDES ENTREPRISES')
            
        Returns:
            Liste des brigades avec leurs informations
        """
        sous_structure_path = self.root_dir / programme_name / structure_code / sous_structure_name
        if not sous_structure_path.exists():
            raise FileNotFoundError(f"Sous-structure non trouvée: {sous_structure_name}")
        
        brigades = []
        for item in sous_structure_path.iterdir():
            if item.is_dir():
                # Compter les fichiers dans la brigade
                files = [f for f in item.iterdir() if f.is_file()]
                xlsx_files = [f for f in files if f.suffix.lower() == '.xlsx']
                png_files = [f for f in files if f.suffix.lower() == '.png']
                
                brigades.append({
                    "name": item.name,
                    "path": str(item),
                    "total_files": len(files),
                    "xlsx_count": len(xlsx_files),
                    "png_count": len(png_files),
                    "contribuables_count": len(xlsx_files)
                })
        
        brigades.sort(key=lambda x: x["name"])
        return brigades
    
    # ========== FONCTIONS DE LISTING - NIVEAU 4: FICHIERS ==========
    
    def list_files_in_brigade(self, programme_name: str, structure_code: str,
                              sous_structure_name: str, brigade_name: str) -> List[Dict[str, Any]]:
        """
        Liste tous les fichiers dans une brigade donnée.
        
        Args:
            programme_name: Nom du programme
            structure_code: Code de la structure
            sous_structure_name: Nom de la sous-structure
            brigade_name: Nom de la brigade
            
        Returns:
            Liste des fichiers avec leurs informations
        """
        brigade_path = (self.root_dir / programme_name / structure_code / 
                       sous_structure_name / brigade_name)
        
        if not brigade_path.exists():
            raise FileNotFoundError(f"Brigade non trouvée: {brigade_name}")
        
        files = []
        for item in brigade_path.iterdir():
            if item.is_file():
                file_info = self._get_file_info(item)
                files.append(file_info)
        
        files.sort(key=lambda x: x["name"])
        return files
    
    def list_contribuables_in_brigade(self, programme_name: str, structure_code: str,
                                      sous_structure_name: str, brigade_name: str) -> List[Dict[str, Any]]:
        """
        Liste tous les contribuables (basé sur les fichiers xlsx) dans une brigade.
        
        Args:
            programme_name: Nom du programme
            structure_code: Code de la structure
            sous_structure_name: Nom de la sous-structure
            brigade_name: Nom de la brigade
            
        Returns:
            Liste des contribuables avec leurs fichiers associés
        """
        brigade_path = (self.root_dir / programme_name / structure_code / 
                       sous_structure_name / brigade_name)
        
        if not brigade_path.exists():
            raise FileNotFoundError(f"Brigade non trouvée: {brigade_name}")
        
        # Regrouper les fichiers par IFU
        contribuables = {}
        for item in brigade_path.iterdir():
            if item.is_file():
                file_info = self._get_file_info(item)
                ifu = file_info.get("ifu")
                
                if ifu:
                    if ifu not in contribuables:
                        contribuables[ifu] = {
                            "ifu": ifu,
                            "xlsx": None,
                            "chart_png": None,
                            "forecast_png": None,
                            "files": []
                        }
                    
                    contribuables[ifu]["files"].append(file_info)
                    
                    if file_info["file_type"] == "xlsx":
                        contribuables[ifu]["xlsx"] = file_info
                    elif file_info["file_type"] == "chart":
                        contribuables[ifu]["chart_png"] = file_info
                    elif file_info["file_type"] == "forecast":
                        contribuables[ifu]["forecast_png"] = file_info
        
        return list(contribuables.values())
    
    # ========== FONCTIONS DE RECHERCHE ==========
    
    def search_by_ifu(self, ifu: str, programme_name: str = None) -> List[Dict[str, Any]]:
        """
        Recherche tous les fichiers d'un contribuable par son IFU.
        
        Args:
            ifu: IFU du contribuable
            programme_name: Optionnel - limiter la recherche à un programme
            
        Returns:
            Liste des fichiers trouvés
        """
        results = []
        search_path = self.root_dir / programme_name if programme_name else self.root_dir
        
        if not search_path.exists():
            return results
        
        for file_path in search_path.rglob(f"{ifu}*"):
            if file_path.is_file():
                file_info = self._get_file_info(file_path)
                file_info["programme"] = self._extract_programme_from_path(file_path)
                results.append(file_info)
        
        return results
    
    def search_files(self, pattern: str, programme_name: str = None, 
                    extension: str = None) -> List[Dict[str, Any]]:
        """
        Recherche des fichiers par pattern dans le nom.
        
        Args:
            pattern: Pattern à rechercher dans le nom de fichier
            programme_name: Optionnel - limiter la recherche à un programme
            extension: Optionnel - filtrer par extension (ex: 'xlsx', 'png')
            
        Returns:
            Liste des fichiers correspondants
        """
        results = []
        search_path = self.root_dir / programme_name if programme_name else self.root_dir
        
        if not search_path.exists():
            return results
        
        glob_pattern = f"*{pattern}*"
        if extension:
            glob_pattern = f"*{pattern}*.{extension}"
        
        for file_path in search_path.rglob(glob_pattern):
            if file_path.is_file():
                file_info = self._get_file_info(file_path)
                file_info["programme"] = self._extract_programme_from_path(file_path)
                results.append(file_info)
        
        return results
    
    # ========== FONCTIONS D'OBTENTION DE FICHIERS SPÉCIFIQUES ==========
    
    def get_contribuable_files(self, ifu: str, programme_name: str, 
                               structure_code: str = None) -> Dict[str, Any]:
        """
        Obtient tous les fichiers associés à un contribuable.
        
        Args:
            ifu: IFU du contribuable
            programme_name: Nom du programme
            structure_code: Optionnel - code de la structure
            
        Returns:
            Dictionnaire contenant les fichiers xlsx, png_chart, png_forecast
        """
        search_path = self.root_dir / programme_name
        if structure_code:
            search_path = search_path / structure_code
        
        result = {
            "ifu": ifu,
            "xlsx": None,
            "chart_png": None,  # Fichier _C.png
            "forecast_png": None,  # Fichier _F.png
            "all_files": []
        }
        
        for file_path in search_path.rglob(f"{ifu}*"):
            if file_path.is_file():
                file_info = self._get_file_info(file_path)
                result["all_files"].append(file_info)
                
                if file_path.suffix.lower() == '.xlsx':
                    result["xlsx"] = file_info
                elif file_path.name.endswith('_C.png'):
                    result["chart_png"] = file_info
                elif file_path.name.endswith('_F.png'):
                    result["forecast_png"] = file_info
        
        return result
    
    def get_file_path(self, programme_name: str, structure_code: str,
                      sous_structure_name: str, brigade_name: str, 
                      filename: str) -> str:
        """
        Obtient le chemin complet d'un fichier pour le servir.
        
        Args:
            programme_name: Nom du programme
            structure_code: Code de la structure
            sous_structure_name: Nom de la sous-structure
            brigade_name: Nom de la brigade
            filename: Nom du fichier
            
        Returns:
            Chemin absolu du fichier
        """
        file_path = (self.root_dir / programme_name / structure_code / 
                    sous_structure_name / brigade_name / filename)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Fichier non trouvé: {filename}")
        
        return str(file_path.resolve())
    
    # ========== FONCTIONS DE STATISTIQUES ==========
    
    def get_programme_stats(self, programme_name: str) -> Dict[str, Any]:
        """
        Obtient les statistiques d'un programme.
        
        Args:
            programme_name: Nom du programme
            
        Returns:
            Dictionnaire avec les statistiques détaillées
        """
        programme_path = self.root_dir / programme_name
        if not programme_path.exists():
            raise FileNotFoundError(f"Programme non trouvé: {programme_name}")
        
        stats = {
            "programme": programme_name,
            "total_structures": 0,
            "total_sous_structures": 0,
            "total_brigades": 0,
            "total_files": 0,
            "total_xlsx": 0,
            "total_png": 0,
            "total_contribuables": 0,
            "structures_stats": []
        }
        
        for structure in programme_path.iterdir():
            if structure.is_dir():
                stats["total_structures"] += 1
                
                structure_stats = {
                    "code": structure.name,
                    "sous_structures_count": 0,
                    "brigades_count": 0,
                    "files_count": 0,
                    "contribuables_count": 0
                }
                
                xlsx_files = list(structure.rglob("*.xlsx"))
                png_files = list(structure.rglob("*.png"))
                
                structure_stats["files_count"] = len(xlsx_files) + len(png_files)
                structure_stats["contribuables_count"] = len(xlsx_files)
                
                # Compter les sous-structures et brigades
                for sous_structure in structure.iterdir():
                    if sous_structure.is_dir():
                        structure_stats["sous_structures_count"] += 1
                        stats["total_sous_structures"] += 1
                        
                        for brigade in sous_structure.iterdir():
                            if brigade.is_dir():
                                structure_stats["brigades_count"] += 1
                                stats["total_brigades"] += 1
                
                stats["total_files"] += structure_stats["files_count"]
                stats["total_xlsx"] += len(xlsx_files)
                stats["total_png"] += len(png_files)
                stats["total_contribuables"] += structure_stats["contribuables_count"]
                stats["structures_stats"].append(structure_stats)
        
        return stats
    
    def get_global_stats(self) -> Dict[str, Any]:
        """
        Obtient les statistiques globales de tous les programmes.
        
        Returns:
            Dictionnaire avec les statistiques globales
        """
        stats = {
            "total_programmes": 0,
            "total_structures": 0,
            "total_files": 0,
            "total_contribuables": 0,
            "programmes": []
        }
        
        for programme in self.list_programmes():
            stats["total_programmes"] += 1
            prog_stats = self.get_programme_stats(programme["name"])
            
            stats["total_structures"] += prog_stats["total_structures"]
            stats["total_files"] += prog_stats["total_files"]
            stats["total_contribuables"] += prog_stats["total_contribuables"]
            
            stats["programmes"].append({
                "name": programme["name"],
                "date": programme["date"],
                "stats": prog_stats
            })
        
        return stats
    
    def get_structure_stats(self, programme_name: str, structure_code: str) -> Dict[str, Any]:
        """
        Obtient les statistiques détaillées d'une structure.
        
        Args:
            programme_name: Nom du programme
            structure_code: Code de la structure
            
        Returns:
            Dictionnaire avec les statistiques de la structure
        """
        structure_path = self.root_dir / programme_name / structure_code
        if not structure_path.exists():
            raise FileNotFoundError(f"Structure non trouvée: {structure_code}")
        
        stats = {
            "structure": structure_code,
            "programme": programme_name,
            "total_sous_structures": 0,
            "total_brigades": 0,
            "total_files": 0,
            "total_contribuables": 0,
            "sous_structures_stats": []
        }
        
        for sous_structure in structure_path.iterdir():
            if sous_structure.is_dir():
                stats["total_sous_structures"] += 1
                
                ss_stats = {
                    "name": sous_structure.name,
                    "brigades_count": 0,
                    "files_count": 0,
                    "contribuables_count": 0,
                    "brigades": []
                }
                
                for brigade in sous_structure.iterdir():
                    if brigade.is_dir():
                        ss_stats["brigades_count"] += 1
                        stats["total_brigades"] += 1
                        
                        files = list(brigade.iterdir())
                        xlsx_count = len([f for f in files if f.is_file() and f.suffix.lower() == '.xlsx'])
                        
                        ss_stats["brigades"].append({
                            "name": brigade.name,
                            "files_count": len([f for f in files if f.is_file()]),
                            "contribuables_count": xlsx_count
                        })
                        
                        ss_stats["files_count"] += len([f for f in files if f.is_file()])
                        ss_stats["contribuables_count"] += xlsx_count
                
                stats["total_files"] += ss_stats["files_count"]
                stats["total_contribuables"] += ss_stats["contribuables_count"]
                stats["sous_structures_stats"].append(ss_stats)
        
        return stats
    
    # ========== FONCTIONS UTILITAIRES ==========
    
    def _get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """
        Extrait les informations d'un fichier.
        
        Args:
            file_path: Chemin du fichier
            
        Returns:
            Dictionnaire avec les informations du fichier
        """
        stat = file_path.stat()
        
        # Déterminer le type de fichier
        if file_path.suffix.lower() == '.xlsx':
            file_type = 'xlsx'
        elif file_path.name.endswith('_C.png'):
            file_type = 'chart'
        elif file_path.name.endswith('_F.png'):
            file_type = 'forecast'
        elif file_path.suffix.lower() == '.png':
            file_type = 'image'
        else:
            file_type = 'other'
        
        # Extraire l'IFU du nom de fichier (9 premiers caractères)
        ifu = None
        if len(file_path.stem) >= 9:
            potential_ifu = file_path.stem[:9]
            # Vérifier si ça ressemble à un IFU (chiffres + lettre)
            if potential_ifu[:-1].isdigit() and potential_ifu[-1].isalpha():
                ifu = potential_ifu
        
        return {
            "name": file_path.name,
            "path": str(file_path),
            "relative_path": str(file_path.relative_to(self.root_dir)),
            "extension": file_path.suffix.lower(),
            "size": stat.st_size,
            "size_formatted": self._format_size(stat.st_size),
            "modified_date": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "file_type": file_type,
            "ifu": ifu
        }
    
    def _format_size(self, size: int) -> str:
        """Formate la taille en unité lisible"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"
    
    def _extract_programme_from_path(self, file_path: Path) -> Optional[str]:
        """Extrait le nom du programme depuis le chemin d'un fichier"""
        try:
            relative = file_path.relative_to(self.root_dir)
            parts = relative.parts
            if parts:
                return parts[0]
        except ValueError:
            pass
        return None
    
    def extract_location_from_path(self, file_path: str) -> Dict[str, Optional[str]]:
        """
        Extrait les informations de localisation complètes depuis un chemin de fichier.
        
        Args:
            file_path: Chemin du fichier (absolu ou relatif)
            
        Returns:
            Dictionnaire avec programme, structure, sous_structure, brigade
        """
        try:
            path = Path(file_path)
            if path.is_absolute():
                relative = path.relative_to(self.root_dir)
            else:
                relative = path
            
            parts = relative.parts
            
            return {
                "programme": parts[0] if len(parts) > 0 else None,
                "structure": parts[1] if len(parts) > 1 else None,
                "sous_structure": parts[2] if len(parts) > 2 else None,
                "brigade": parts[3] if len(parts) > 3 else None,
                "filename": parts[4] if len(parts) > 4 else None
            }
        except (ValueError, IndexError):
            return {
                "programme": None,
                "structure": None,
                "sous_structure": None,
                "brigade": None,
                "filename": None
            }
    
    def get_full_path(self, relative_path: str) -> str:
        """
        Convertit un chemin relatif en chemin absolu.
        
        Args:
            relative_path: Chemin relatif depuis le dossier programmes
            
        Returns:
            Chemin absolu
        """
        full_path = self.root_dir / relative_path
        if not full_path.exists():
            raise FileNotFoundError(f"Chemin non trouvé: {relative_path}")
        return str(full_path.resolve())
    
    def path_exists(self, relative_path: str) -> bool:
        """
        Vérifie si un chemin existe.
        
        Args:
            relative_path: Chemin relatif depuis le dossier programmes
            
        Returns:
            True si le chemin existe
        """
        return (self.root_dir / relative_path).exists()


# Instance singleton pour utilisation dans les endpoints
_dir_reader_instance: Optional[DirReader] = None


def get_dir_reader(root_path: str = None) -> DirReader:
    """
    Obtient l'instance singleton du DirReader.
    
    Args:
        root_path: Chemin optionnel vers le dossier programmes
        
    Returns:
        Instance de DirReader
    """
    global _dir_reader_instance
    if _dir_reader_instance is None:
        _dir_reader_instance = DirReader(root_path)
    return _dir_reader_instance
