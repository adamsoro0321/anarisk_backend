"""
ANARISK API Package
Contient les blueprints pour l'API REST
"""

from flask import Blueprint

# Import des blueprints existants
from .risk_data import risk_bp
from .auth import auth_bp
from .stats import stats_bp
from .api import api_bp
from .fiche_reader import fiche_bp
from .user import users_bp
from .contribuable_api import contribuable_bp
from .brigade_api import brigade_bp
from .quantume_api import quantume_bp
from .programmes import programmes_files_bp
from .indicator import indicateur_bp
# Liste des blueprints à enregistrer
__all__ = [
    'risk_bp', 'auth_bp', 'stats_bp', 'api_bp',
    'users_bp', 'contribuable_bp', 'fiche_bp', 
    'programmes_files_bp', 'brigade_bp', 'quantume_bp',
    'indicateur_bp'
]


def register_blueprints(app):
    """
    Enregistre tous les blueprints de l'API avec le préfixe /api/v1
    
    Args:
        app: Instance de l'application Flask
    """
    # Blueprints existants
    app.register_blueprint(risk_bp, url_prefix='/api/v1')
    app.register_blueprint(auth_bp, url_prefix='/api/v1')
    app.register_blueprint(stats_bp, url_prefix='/api/v1')
    app.register_blueprint(api_bp, url_prefix='/api/v1')
    app.register_blueprint(fiche_bp, url_prefix='/api/v1')
    app.register_blueprint(users_bp, url_prefix='/api/v1')
    app.register_blueprint(contribuable_bp, url_prefix='/api/v1')
    app.register_blueprint(brigade_bp, url_prefix='/api/v1')
    app.register_blueprint(quantume_bp, url_prefix='/api/v1')
    app.register_blueprint(programmes_files_bp, url_prefix='/api/v1')
    app.register_blueprint(indicateur_bp, url_prefix='/api/v1')
    print("✓ Blueprints API enregistrés avec succès")
