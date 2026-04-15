"""
Blueprint pour la lecture des fiches
Gère les endpoints d'exploration des dossiers fiches, structures, sous-structures et brigades
"""
from flask import Blueprint, jsonify, request, current_app, send_file
import os
import zipfile
from io import BytesIO
from pathlib import Path
from dir_reader import get_dir_reader, DirReader
from api_utils.utils import token_required

fiche_bp = Blueprint('fiches', __name__)


def get_reader() -> DirReader:
    """Obtient l'instance du DirReader"""
    try:
        return get_dir_reader()
    except FileNotFoundError as e:
        current_app.logger.error(f"Dossier fiches non trouvé: {str(e)}")
        raise


# ========== ENDPOINTS NIVEAU 0: PROGRAMMES ==========

@fiche_bp.route('/fiches', methods=['GET'])
@token_required
def list_programmes():
    """
    Liste tous les fiches disponibles
    
    Returns:
        JSON avec la liste des fiches
    """
    try:
        reader = get_reader()
        fiches = reader.list_programmes()
        
        return jsonify({
            'success': True,
            'count': len(fiches),
            'data': fiches
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'DIRECTORY_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_programmes: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS NIVEAU 1: STRUCTURES ==========

@fiche_bp.route('/fiches/<programme_name>/structures', methods=['GET'])
@token_required
def list_structures(programme_name: str):
    """
    Liste toutes les structures d'un programme
    
    Args:
        programme_name: Nom du programme (ex: 'programme_2025_12_25')
    
    Returns:
        JSON avec la liste des structures (filtrées par UR si l'utilisateur est un UR)
    """
    try:
        reader = get_reader()
        structures = reader.list_structures(programme_name)
        
        # Filtrer par UR si l'utilisateur est un UR
        user_ur = getattr(request, 'user_ur', None)
        if user_ur:
            # Filtrer les structures pour ne retourner que celle correspondant à l'UR
            structures = [s for s in structures if s.get('code') == user_ur]
            current_app.logger.info(f"Structures filtrées pour UR: {user_ur}")
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'count': len(structures),
            'data': structures,
            'filtered_by_ur': user_ur is not None
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'PROGRAMME_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_structures: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS NIVEAU 2: SOUS-STRUCTURES ==========

@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures', methods=['GET'])
@token_required
def list_sous_structures(programme_name: str, structure_code: str):
    """
    Liste toutes les sous-structures d'une structure
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure (ex: 'DGE', 'DME_CI')
    
    Returns:
        JSON avec la liste des sous-structures
    """
    try:
        reader = get_reader()
        sous_structures = reader.list_sous_structures(programme_name, structure_code)
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'structure': structure_code,
            'count': len(sous_structures),
            'data': sous_structures
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'STRUCTURE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_sous_structures: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS NIVEAU 3: BRIGADES ==========

@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures/<sous_structure_name>/brigades', methods=['GET'])
@token_required
def list_brigades(programme_name: str, structure_code: str, sous_structure_name: str):
    """
    Liste toutes les brigades d'une sous-structure
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
        sous_structure_name: Nom de la sous-structure
    
    Returns:
        JSON avec la liste des brigades (filtrées par brigade si l'utilisateur est une brigade)
    """
    try:
        reader = get_reader()
        brigades = reader.list_brigades(programme_name, structure_code, sous_structure_name)
        
        # Filtrer par brigade si l'utilisateur est une brigade
        user_brigade = getattr(request, 'user_brigade', None)
        if user_brigade:
            # Filtrer les brigades pour ne retourner que celle correspondant à la brigade de l'utilisateur
            brigades = [b for b in brigades if b.get('name') == user_brigade]
            current_app.logger.info(f"Brigades filtrées pour brigade: {user_brigade}")
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'structure': structure_code,
            'sous_structure': sous_structure_name,
            'count': len(brigades),
            'data': brigades,
            'filtered_by_brigade': user_brigade is not None
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'SOUS_STRUCTURE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_brigades: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures/<sous_structure_name>/brigades/<brigade_name>/download-zip', methods=['GET'])
@token_required
def download_brigade_zip(programme_name: str, structure_code: str, 
                         sous_structure_name: str, brigade_name: str):
    """
    Télécharge tous les fichiers d'une brigade en format ZIP
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
        sous_structure_name: Nom de la sous-structure
        brigade_name: Nom de la brigade
    
    Returns:
        Fichier ZIP contenant tous les fichiers de la brigade
    """
    try:
        reader = get_reader()
        
        # Récupérer tous les fichiers de la brigade
        files = reader.list_files_in_brigade(
            programme_name, structure_code, sous_structure_name, brigade_name
        )
        
        if not files:
            return jsonify({
                'success': False,
                'message': 'Aucun fichier trouvé dans cette brigade',
                'error_code': 'NO_FILES_FOUND'
            }), 404
        
        # Créer le ZIP en mémoire
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_info in files:
                file_path = Path(file_info['path'])
                
                if file_path.exists() and file_path.is_file():
                    # Ajouter le fichier au ZIP avec son nom simple
                    zip_file.write(file_path, arcname=file_info['name'])
                    current_app.logger.info(f"Ajout du fichier au ZIP: {file_info['name']}")
        
        # Préparer le buffer pour l'envoi
        zip_buffer.seek(0)
        
        # Nom du fichier ZIP descriptif: brigade_structure_programme.zip
        zip_filename = f"{brigade_name}_{structure_code}_{programme_name}.zip"
        
        current_app.logger.info(f"ZIP créé avec {len(files)} fichiers: {zip_filename}")
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=zip_filename
        )
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'BRIGADE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans download_brigade_zip: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la création du fichier ZIP',
            'error_code': 'ZIP_CREATION_ERROR'
        }), 500


# ========== ENDPOINTS NIVEAU 4: FICHIERS ==========

@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures/<sous_structure_name>/brigades/<brigade_name>/files', methods=['GET'])
@token_required
def list_files_in_brigade(programme_name: str, structure_code: str, 
                          sous_structure_name: str, brigade_name: str):
    """
    Liste tous les fichiers d'une brigade
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
        sous_structure_name: Nom de la sous-structure
        brigade_name: Nom de la brigade
    
    Returns:
        JSON avec la liste des fichiers
    """
    try:
        reader = get_reader()
        files = reader.list_files_in_brigade(
            programme_name, structure_code, sous_structure_name, brigade_name
        )
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'structure': structure_code,
            'sous_structure': sous_structure_name,
            'brigade': brigade_name,
            'count': len(files),
            'data': files
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'BRIGADE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_files_in_brigade: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures/<sous_structure_name>/brigades/<brigade_name>/contribuables', methods=['GET'])
@token_required
def list_contribuables_in_brigade(programme_name: str, structure_code: str,
                                  sous_structure_name: str, brigade_name: str):
    """
    Liste tous les contribuables d'une brigade (groupés par IFU)
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
        sous_structure_name: Nom de la sous-structure
        brigade_name: Nom de la brigade
    
    Returns:
        JSON avec la liste des contribuables et leurs fichiers
    """
    try:
        reader = get_reader()
        contribuables = reader.list_contribuables_in_brigade(
            programme_name, structure_code, sous_structure_name, brigade_name
        )
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'structure': structure_code,
            'sous_structure': sous_structure_name,
            'brigade': brigade_name,
            'count': len(contribuables),
            'data': contribuables
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'BRIGADE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_contribuables_in_brigade: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS DE RECHERCHE ==========

@fiche_bp.route('/fiches/search/ifu/<ifu>', methods=['GET'])
@token_required
def search_by_ifu(ifu: str):
    """
    Recherche tous les fichiers d'un contribuable par son IFU
    
    Args:
        ifu: IFU du contribuable
    
    Query Parameters:
        - programme (str): Optionnel - limiter la recherche à un programme
    
    Returns:
        JSON avec les fichiers trouvés
    """
    try:
        reader = get_reader()
        programme_name = request.args.get('programme', None)
        
        results = reader.search_by_ifu(ifu, programme_name)
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'programme_filter': programme_name,
            'count': len(results),
            'data': results
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans search_by_ifu: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@fiche_bp.route('/fiches/search/files', methods=['GET'])
@token_required
def search_files():
    """
    Recherche des fichiers par pattern
    
    Query Parameters:
        - pattern (str): Pattern à rechercher (requis)
        - programme (str): Optionnel - limiter la recherche à un programme
        - extension (str): Optionnel - filtrer par extension (ex: 'xlsx', 'png')
    
    Returns:
        JSON avec les fichiers correspondants
    """
    try:
        pattern = request.args.get('pattern', None)
        
        if not pattern:
            return jsonify({
                'success': False,
                'message': 'Le paramètre "pattern" est requis',
                'error_code': 'MISSING_PARAMETER'
            }), 400
        
        reader = get_reader()
        programme_name = request.args.get('programme', None)
        extension = request.args.get('extension', None)
        
        results = reader.search_files(pattern, programme_name, extension)
        
        return jsonify({
            'success': True,
            'pattern': pattern,
            'programme_filter': programme_name,
            'extension_filter': extension,
            'count': len(results),
            'data': results
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans search_files: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS POUR FICHIERS CONTRIBUABLE ==========

@fiche_bp.route('/fiches/<programme_name>/contribuable/<ifu>/files', methods=['GET'])
@token_required
def get_contribuable_files(programme_name: str, ifu: str):
    """
    Obtient tous les fichiers associés à un contribuable
    
    Args:
        programme_name: Nom du programme
        ifu: IFU du contribuable
    
    Query Parameters:
        - structure (str): Optionnel - code de la structure pour filtrer
    
    Returns:
        JSON avec les fichiers du contribuable (xlsx, chart_png, forecast_png)
    """
    try:
        reader = get_reader()
        structure_code = request.args.get('structure', None)
        
        result = reader.get_contribuable_files(ifu, programme_name, structure_code)
        
        return jsonify({
            'success': True,
            'programme': programme_name,
            'structure_filter': structure_code,
            'data': result
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_contribuable_files: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINTS STATISTIQUES ==========

@fiche_bp.route('/fiches/stats', methods=['GET'])
@token_required
def get_global_stats():
    """
    Obtient les statistiques globales de tous les fiches
    
    Returns:
        JSON avec les statistiques globales
    """
    try:
        reader = get_reader()
        stats = reader.get_global_stats()
        
        return jsonify({
            'success': True,
            'data': stats
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_global_stats: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@fiche_bp.route('/fiches/<programme_name>/stats', methods=['GET'])
@token_required
def get_programme_stats(programme_name: str):
    """
    Obtient les statistiques d'un programme
    
    Args:
        programme_name: Nom du programme
    
    Returns:
        JSON avec les statistiques du programme
    """
    try:
        reader = get_reader()
        stats = reader.get_programme_stats(programme_name)
        
        return jsonify({
            'success': True,
            'data': stats
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'PROGRAMME_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_programme_stats: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/stats', methods=['GET'])
@token_required
def get_structure_stats(programme_name: str, structure_code: str):
    """
    Obtient les statistiques d'une structure
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
    
    Returns:
        JSON avec les statistiques de la structure
    """
    try:
        reader = get_reader()
        stats = reader.get_structure_stats(programme_name, structure_code)
        
        return jsonify({
            'success': True,
            'data': stats
        }), 200
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'STRUCTURE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_structure_stats: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


# ========== ENDPOINT POUR SERVIR LES FICHIERS ==========

@fiche_bp.route('/fiches/<programme_name>/structures/<structure_code>/sous-structures/<sous_structure_name>/brigades/<brigade_name>/files/<filename>', methods=['GET'])
@token_required
def download_file(programme_name: str, structure_code: str, 
                  sous_structure_name: str, brigade_name: str, filename: str):
    """
    Télécharge un fichier spécifique
    
    Args:
        programme_name: Nom du programme
        structure_code: Code de la structure
        sous_structure_name: Nom de la sous-structure
        brigade_name: Nom de la brigade
        filename: Nom du fichier
    
    Returns:
        Le fichier demandé
    """
    try:
        reader = get_reader()
        file_path = reader.get_file_path(
            programme_name, structure_code, sous_structure_name, brigade_name, filename
        )
        
        # Déterminer le type MIME
        if filename.endswith('.xlsx'):
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        elif filename.endswith('.png'):
            mimetype = 'image/png'
        else:
            mimetype = 'application/octet-stream'
        
        return send_file(
            file_path,
            mimetype=mimetype,
            as_attachment=False,
            download_name=filename
        )
        
    except FileNotFoundError as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'error_code': 'FILE_NOT_FOUND'
        }), 404
    except Exception as e:
        current_app.logger.error(f"Erreur dans download_file: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500



