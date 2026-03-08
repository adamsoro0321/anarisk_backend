from flask import Blueprint, jsonify, request, current_app
import pandas as pd
import os
import numpy as np
from utils.util import get_latest_risk_file
risk_bp = Blueprint('risk', __name__)


# Variable globale pour les données de risque
file_name = get_latest_risk_file()
_risk_data_df = None
risk_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                'data','risk_contribuables',
                file_name
            )
print(f"\n Risk data file path 22: {risk_file_path}")

def _load_risk_data():
    """Charge les données de risque depuis le fichier CSV"""
    global _risk_data_df
    if _risk_data_df is None or _risk_data_df.empty:
            if os.path.exists(risk_file_path):
                try:
                    _risk_data_df = pd.read_csv(risk_file_path, sep=';', encoding='utf-8')
                    print(f"Risk data loaded: {_risk_data_df.shape}")
                except Exception as e:
                    print(f"Error loading risk data: {str(e)}")
    return _risk_data_df


def get_risk_dataframe():
    """Retourne le DataFrame des données de risque"""
    return _load_risk_data()



@risk_bp.route('/risk-data', methods=['GET'])
def get_risk_data():
    """
    Renvoie les données de risque avec pagination.
    
    Query Parameters:
        - page (int): Numéro de la page (défaut: 1)
        - per_page (int): Nombre d'éléments par page (défaut: 100, max: 1000)
        - sort_by (str): Colonne de tri (optionnel)
        - sort_order (str): Ordre de tri 'asc' ou 'desc' (défaut: 'desc')
    
    Returns:
        JSON avec les données paginées et les métadonnées de pagination
    """
    try:
        # Charger les données de risque
        risk_data_df = get_risk_dataframe()
        
        # Vérifier si les données sont disponibles
        if risk_data_df is None or risk_data_df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404

        # Récupération des paramètres de pagination
        try:
            page = int(request.args.get('page', 1))
            per_page = int(request.args.get('per_page', 10))
        except ValueError:
            return jsonify({
                'success': False,
                'message': 'Les paramètres page et per_page doivent être des entiers',
                'error_code': 'INVALID_PAGINATION_PARAMS'
            }), 400
        
        # Validation des paramètres de pagination
        if page < 1:
            return jsonify({
                'success': False,
                'message': 'Le numéro de page doit être supérieur ou égal à 1',
                'error_code': 'INVALID_PAGE_NUMBER'
            }), 400
        
        if per_page < 1 or per_page > 1000:
            return jsonify({
                'success': False,
                'message': 'Le nombre d\'éléments par page doit être entre 1 et 1000',
                'error_code': 'INVALID_PER_PAGE'
            }), 400
        
        # Paramètres de tri
        sort_by = request.args.get('sort_by', None)
        sort_order = request.args.get('sort_order', 'desc').lower()
        
        if sort_order not in ['asc', 'desc']:
            return jsonify({
                'success': False,
                'message': 'L\'ordre de tri doit être "asc" ou "desc"',
                'error_code': 'INVALID_SORT_ORDER'
            }), 400
        
        # Copie du DataFrame pour ne pas modifier l'original
        df = risk_data_df.copy()
        
        # Tri si demandé
        if sort_by:
            if sort_by not in df.columns:
                return jsonify({
                    'success': False,
                    'message': f'La colonne de tri "{sort_by}" n\'existe pas',
                    'available_columns': list(df.columns),
                    'error_code': 'INVALID_SORT_COLUMN'
                }), 400
            df = df.sort_values(by=sort_by, ascending=(sort_order == 'asc'))
        
        # Calcul de la pagination
        total_records = len(df)
        total_pages = (total_records + per_page - 1) // per_page  # Arrondi supérieur

        # Vérification que la page demandée existe
        if page > total_pages and total_records > 0:
            return jsonify({
                'success': False,
                'message': f'La page {page} n\'existe pas. Nombre total de pages: {total_pages}',
                'error_code': 'PAGE_NOT_FOUND'
            }), 404

        # Extraction des données pour la page demandée
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_df = df.iloc[start_idx:end_idx]
        
        # Remplacement des NaN par None (null en JSON)
        paginated_df = paginated_df.replace({pd.NA: None, pd.NaT: None})
        paginated_df = paginated_df.where(pd.notnull(paginated_df), None)
        
        # Remplacer les NaN par None (devient null en JSON)
        paginated_df = paginated_df.replace({np.nan: None})
        # Conversion en dictionnaire
        data = paginated_df.to_dict(orient='records')
        
        # Construction de la réponse avec métadonnées de pagination
        response = {
            'success': True,
           'data': data,
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_records': total_records,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1,
                'next_page': page + 1 if page < total_pages else None,
                'prev_page': page - 1 if page > 1 else None
            },
            'meta': {
                'columns': list(df.columns),
                'sort_by': sort_by,
                'sort_order': sort_order
            }
        }
        
        return jsonify(response), 200
        
    except pd.errors.EmptyDataError:
        return jsonify({
            'success': False,
            'message': 'Les données de risque sont vides',
            'error_code': 'EMPTY_DATA'
        }), 404
        
    except MemoryError:
        return jsonify({
            'success': False,
            'message': 'Mémoire insuffisante pour traiter la requête',
            'error_code': 'MEMORY_ERROR'
        }), 503
        
    except Exception as e:
        # Log de l'erreur pour le débogage
     
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR',

        }), 500

