"""
Blueprint pour la gestion des programmes (fichiers Excel)
Gère les endpoints pour lister, téléverser, télécharger et supprimer des fichiers Excel
"""
from flask import Blueprint, jsonify, request, current_app, send_file
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import shutil

programmes_files_bp = Blueprint('programmes_files', __name__)


def get_programmes_directory():
    """
    Obtient le chemin du dossier programmes
    
    Returns:
        str: Chemin absolu du dossier programmes
    """
    programmes_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'programmes'
    )
    
    # Créer le dossier s'il n'existe pas
    os.makedirs(programmes_dir, exist_ok=True)
    
    return programmes_dir


def get_file_info(file_path):
    """
    Récupère les informations d'un fichier
    
    Args:
        file_path: Chemin complet du fichier
    
    Returns:
        dict: Informations du fichier
    """
    try:
        stat_info = os.stat(file_path)
        file_size = stat_info.st_size
        modified_time = datetime.fromtimestamp(stat_info.st_mtime)
        
        # Formater la taille du fichier
        if file_size < 1024:
            size_formatted = f"{file_size} B"
        elif file_size < 1024 * 1024:
            size_formatted = f"{file_size / 1024:.2f} KB"
        else:
            size_formatted = f"{file_size / (1024 * 1024):.2f} MB"
        
        return {
            'name': os.path.basename(file_path),
            'path': file_path,
            'size': file_size,
            'size_formatted': size_formatted,
            'modified_date': modified_time.isoformat(),
            'extension': os.path.splitext(file_path)[1].lower()
        }
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des infos du fichier: {str(e)}")
        return None


# ========== ENDPOINT: LISTER LES PROGRAMMES (FICHIERS EXCEL) ==========

@programmes_files_bp.route('/programmes-files', methods=['GET'])
def list_programmes_files():
    """
    Liste tous les fichiers Excel dans le dossier programmes
    
    Returns:
        JSON avec la liste des fichiers Excel
    """
    try:
        programmes_dir = get_programmes_directory()
        
        # Extensions Excel valides
        excel_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb'}
        
        # Lister tous les fichiers Excel
        excel_files = []
        
        try:
            for filename in os.listdir(programmes_dir):
                file_path = os.path.join(programmes_dir, filename)
                
                # Vérifier que c'est un fichier et non un dossier
                if os.path.isfile(file_path):
                    _, ext = os.path.splitext(filename)
                    
                    # Vérifier l'extension
                    if ext.lower() in excel_extensions:
                        file_info = get_file_info(file_path)
                        if file_info:
                            excel_files.append(file_info)
        except Exception as e:
            current_app.logger.error(f"Erreur lors de la lecture du dossier programmes: {str(e)}")
        
        # Trier par date de modification (plus récent en premier)
        excel_files.sort(key=lambda x: x['modified_date'], reverse=True)
        
        return jsonify({
            'success': True,
            'count': len(excel_files),
            'data': excel_files,
            'directory': programmes_dir
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_programmes_files: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500


# ========== ENDPOINT: TÉLÉVERSER UN FICHIER EXCEL ==========

@programmes_files_bp.route('/upload-programme-file', methods=['POST'])
def upload_programme_file():
    """
    Téléverse un fichier Excel dans le dossier programmes
    
    Expects:
        - file: Fichier Excel (.xlsx, .xls, .xlsm, .xlsb)
        - overwrite (optional): true pour écraser un fichier existant
    
    Returns:
        JSON avec le résultat de l'upload
    """
    try:
        # Vérifier qu'un fichier a été envoyé
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'Aucun fichier fourni',
                'error_code': 'NO_FILE'
            }), 400
        
        file = request.files['file']
        
        # Vérifier que le fichier a un nom
        if file.filename == '' or file.filename is None:
            return jsonify({
                'success': False,
                'message': 'Nom de fichier vide',
                'error_code': 'EMPTY_FILENAME'
            }), 400
        
        # Vérifier l'extension
        allowed_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb'}
        filename = secure_filename(file.filename)
        file_ext = os.path.splitext(filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            return jsonify({
                'success': False,
                'message': f'Extension non autorisée. Extensions acceptées: {", ".join(allowed_extensions)}',
                'error_code': 'INVALID_EXTENSION'
            }), 400
        
        # Obtenir le dossier programmes
        programmes_dir = get_programmes_directory()
        file_path = os.path.join(programmes_dir, filename)
        
        # Vérifier si le fichier existe déjà
        overwrite = request.form.get('overwrite', 'false').lower() == 'true'
        
        if os.path.exists(file_path) and not overwrite:
            return jsonify({
                'success': False,
                'message': f'Le fichier "{filename}" existe déjà. Utilisez overwrite=true pour le remplacer.',
                'error_code': 'FILE_EXISTS'
            }), 409
        
        # Sauvegarder le fichier
        file.save(file_path)
        
        # Récupérer les infos du fichier
        file_info = get_file_info(file_path)
        
        current_app.logger.info(f"Fichier Excel téléversé: {filename}")
        
        return jsonify({
            'success': True,
            'message': f'Fichier "{filename}" téléversé avec succès',
            'data': file_info
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de l'upload du fichier: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors du téléversement du fichier',
            'error_code': 'UPLOAD_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500


# ========== ENDPOINT: TÉLÉCHARGER UN FICHIER EXCEL ==========

@programmes_files_bp.route('/programmes-files/download/<filename>', methods=['GET'])
def download_programme_file(filename: str):
    """
    Télécharge un fichier Excel du dossier programmes
    
    Args:
        filename: Nom du fichier à télécharger
    
    Returns:
        Le fichier Excel en téléchargement
    """
    try:
        # Sécuriser le nom du fichier
        filename = secure_filename(filename)
        
        programmes_dir = get_programmes_directory()
        file_path = os.path.join(programmes_dir, filename)
        
        # Vérifier que le fichier existe
        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'message': f'Fichier "{filename}" introuvable',
                'error_code': 'FILE_NOT_FOUND'
            }), 404
        
        # Vérifier que c'est bien un fichier
        if not os.path.isfile(file_path):
            return jsonify({
                'success': False,
                'message': f'"{filename}" n\'est pas un fichier valide',
                'error_code': 'NOT_A_FILE'
            }), 400
        
        # Déterminer le mimetype
        file_ext = os.path.splitext(filename)[1].lower()
        mimetype_map = {
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.xlsm': 'application/vnd.ms-excel.sheet.macroEnabled.12',
            '.xlsb': 'application/vnd.ms-excel.sheet.binary.macroEnabled.12'
        }
        mimetype = mimetype_map.get(file_ext, 'application/octet-stream')
        
        return send_file(
            file_path,
            mimetype=mimetype,
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors du téléchargement du fichier: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors du téléchargement du fichier',
            'error_code': 'DOWNLOAD_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500


# ========== ENDPOINT: SUPPRIMER UN FICHIER EXCEL ==========

@programmes_files_bp.route('/programmes-files/<filename>', methods=['DELETE'])
def delete_programme_file(filename: str):
    """
    Supprime un fichier Excel du dossier programmes
    
    Args:
        filename: Nom du fichier à supprimer
    
    Returns:
        JSON avec le résultat de la suppression
    """
    try:
        # Sécuriser le nom du fichier
        filename = secure_filename(filename)
        
        programmes_dir = get_programmes_directory()
        file_path = os.path.join(programmes_dir, filename)
        
        # Vérifier que le fichier existe
        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'message': f'Fichier "{filename}" introuvable',
                'error_code': 'FILE_NOT_FOUND'
            }), 404
        
        # Vérifier que c'est bien un fichier
        if not os.path.isfile(file_path):
            return jsonify({
                'success': False,
                'message': f'"{filename}" n\'est pas un fichier valide',
                'error_code': 'NOT_A_FILE'
            }), 400
        
        # Supprimer le fichier
        os.remove(file_path)
        
        current_app.logger.info(f"Fichier Excel supprimé: {filename}")
        
        return jsonify({
            'success': True,
            'message': f'Fichier "{filename}" supprimé avec succès'
        }), 200
        
    except PermissionError:
        return jsonify({
            'success': False,
            'message': 'Permission refusée pour supprimer le fichier',
            'error_code': 'PERMISSION_DENIED'
        }), 403
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la suppression du fichier: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la suppression du fichier',
            'error_code': 'DELETE_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500


# ========== ENDPOINT: OBTENIR LES INFOS D'UN FICHIER ==========

@programmes_files_bp.route('/programmes-files/<filename>', methods=['GET'])
def get_programme_file_info(filename: str):
    """
    Récupère les informations d'un fichier Excel spécifique
    
    Args:
        filename: Nom du fichier
    
    Returns:
        JSON avec les informations du fichier
    """
    try:
        # Sécuriser le nom du fichier
        filename = secure_filename(filename)
        
        programmes_dir = get_programmes_directory()
        file_path = os.path.join(programmes_dir, filename)
        
        # Vérifier que le fichier existe
        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'message': f'Fichier "{filename}" introuvable',
                'error_code': 'FILE_NOT_FOUND'
            }), 404
        
        # Vérifier que c'est bien un fichier
        if not os.path.isfile(file_path):
            return jsonify({
                'success': False,
                'message': f'"{filename}" n\'est pas un fichier valide',
                'error_code': 'NOT_A_FILE'
            }), 400
        
        # Récupérer les infos
        file_info = get_file_info(file_path)
        
        if not file_info:
            raise Exception("Impossible de récupérer les informations du fichier")
        
        return jsonify({
            'success': True,
            'data': file_info
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des infos du fichier: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des informations',
            'error_code': 'INFO_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500


# ========== ENDPOINT: RENOMMER UN FICHIER ==========

@programmes_files_bp.route('/programmes-files/<filename>/rename', methods=['PUT'])
def rename_programme_file(filename: str):
    """
    Renomme un fichier Excel
    
    Args:
        filename: Nom actuel du fichier
    
    Expects:
        - new_name: Nouveau nom du fichier (dans le body JSON)
    
    Returns:
        JSON avec le résultat du renommage
    """
    try:
        # Sécuriser le nom du fichier
        filename = secure_filename(filename)
        
        # Récupérer le nouveau nom
        data = request.get_json()
        if not data or 'new_name' not in data:
            return jsonify({
                'success': False,
                'message': 'Le nouveau nom du fichier est requis',
                'error_code': 'MISSING_NEW_NAME'
            }), 400
        
        new_name = secure_filename(data['new_name'])
        
        if not new_name:
            return jsonify({
                'success': False,
                'message': 'Nouveau nom de fichier invalide',
                'error_code': 'INVALID_NEW_NAME'
            }), 400
        
        programmes_dir = get_programmes_directory()
        old_path = os.path.join(programmes_dir, filename)
        new_path = os.path.join(programmes_dir, new_name)
        
        # Vérifier que le fichier source existe
        if not os.path.exists(old_path):
            return jsonify({
                'success': False,
                'message': f'Fichier "{filename}" introuvable',
                'error_code': 'FILE_NOT_FOUND'
            }), 404
        
        # Vérifier que le nouveau nom n'existe pas déjà
        if os.path.exists(new_path):
            return jsonify({
                'success': False,
                'message': f'Un fichier nommé "{new_name}" existe déjà',
                'error_code': 'TARGET_FILE_EXISTS'
            }), 409
        
        # Renommer le fichier
        os.rename(old_path, new_path)
        
        # Récupérer les infos du fichier renommé
        file_info = get_file_info(new_path)
        
        current_app.logger.info(f"Fichier renommé: {filename} -> {new_name}")
        
        return jsonify({
            'success': True,
            'message': f'Fichier renommé avec succès: "{new_name}"',
            'data': file_info
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors du renommage du fichier: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur lors du renommage du fichier',
            'error_code': 'RENAME_ERROR',
            'details': str(e) if current_app.debug else None
        }), 500
