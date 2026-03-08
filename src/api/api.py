from flask import Blueprint, jsonify, request, current_app
import pandas as pd
import os
import numpy as np
from datetime import datetime
from utils.util import get_latest_risk_file
api_bp = Blueprint('api', __name__)

# Variable globale pour les données de risque
file_name = get_latest_risk_file()
_risk_data_df = None
risk_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                'data','risk_contribuables',
                file_name
            )

@api_bp.route('/latest-run', methods=['GET'])
def latest_run():
    """
    Retourne les informations sur le dernier fichier de risque traité.
    Extrait la date du nom du fichier au format YYYYMMDD et la retourne formatée.
    """
    try:
        # Extraire la date du nom du fichier (format: RISK_INDICATEUR_CONTRIBUABLES_YYYYMMDD.csv)
        date_str = None
        formatted_date = None
        
        if file_name:
            # Extraire la partie date du nom de fichier (les 8 derniers caractères avant .csv)
            # Example: RISK_INDICATEUR_CONTRIBUABLES_20260114.csv -> 20260114
            import re
            match = re.search(r'_(\d{8})\.csv$', file_name)
            if match:
                date_str = match.group(1)
                # Convertir YYYYMMDD en DD/MM/YYYY
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                formatted_date = date_obj.strftime('%d/%m/%Y')
        
        return jsonify({
            "latest_run": file_name,
            "date_raw": date_str,
            "date_formatted": formatted_date,
            "file_path": risk_file_path
        })
    except Exception as e:
        return jsonify({
            "latest_run": file_name,
            "error": str(e)
        }), 500