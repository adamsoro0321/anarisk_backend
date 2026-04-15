"""
Utilitaires Flask pour l'API ANARISK
Direction Générale des Impôts - Burkina Faso
"""

import logging
from datetime import datetime, timedelta, timezone
from functools import wraps

from flask import request, jsonify, current_app
import jwt
from werkzeug.security import generate_password_hash, check_password_hash


# Configuration du logger
logger = logging.getLogger(__name__)


# ============================================================================
# Utilitaires JWT
# ============================================================================

def generate_token(user_id: int, email: str, role: str, ur: str = None, brigade: str = None) -> str:
    """
    Génère un token JWT pour l'utilisateur
    
    Args:
        user_id: ID de l'utilisateur
        email: Email de l'utilisateur
        role: Rôle de l'utilisateur
        ur: Unité de Renseignement (optionnel)
        brigade: Brigade d'affectation (optionnel)
        
    Returns:
        str: Token JWT encodé
    """
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'ur': ur,
        'brigade': brigade,
        'exp': datetime.now(timezone.utc) + timedelta(hours=current_app.config['JWT_EXPIRATION_HOURS']),
        'iat': datetime.now(timezone.utc)
    }
    token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')
    return token


def decode_token(token: str) -> dict | None:
    """
    Décode et valide un token JWT
    
    Args:
        token: Token JWT à décoder
        
    Returns:
        dict | None: Payload du token si valide, None sinon
    """
    try:
        payload = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token expiré")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Token invalide: {e}")
        return None


def token_required(f):
    """
    Décorateur pour protéger les routes avec JWT
    
    Usage:
        @app.route('/protected')
        @token_required
        def protected_route():
            # Accès aux infos utilisateur via request.user_id, request.user_email, request.user_role
            return jsonify({'message': 'Success'})
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Récupérer le token depuis le header Authorization
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(" ")[1]  # Format: "Bearer <token>"
            except IndexError:
                return jsonify({'message': 'Format du token invalide'}), 401
        
        if not token:
            return jsonify({'message': 'Token manquant'}), 401
        
        payload = decode_token(token)
        if not payload:
            return jsonify({'message': 'Token invalide ou expiré'}), 401
        
        # Ajouter les infos utilisateur à la requête
        request.user_id = payload['user_id']
        request.user_email = payload['email']
        request.user_role = payload['role']
        request.user_ur = payload.get('ur')
        request.user_brigade = payload.get('brigade')
        
        return f(*args, **kwargs)
    return decorated


def role_required(*allowed_roles):
    """
    Décorateur pour restreindre l'accès aux routes selon le rôle
    
    Usage:
        @app.route('/admin')
        @token_required
        @role_required('admin', 'superadmin')
        def admin_route():
            return jsonify({'message': 'Admin access'})
    
    Args:
        *allowed_roles: Rôles autorisés à accéder à la route
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not hasattr(request, 'user_role'):
                return jsonify({'message': 'Authentification requise'}), 401
            
            if request.user_role not in allowed_roles:
                logger.warning(f"Accès refusé pour le rôle: {request.user_role}, rôles autorisés: {allowed_roles}")
                return jsonify({'message': 'Accès non autorisé'}), 403
            
            return f(*args, **kwargs)
        return decorated
    return decorator


# ============================================================================
# Utilitaires de hachage de mot de passe
# ============================================================================

def hash_password(password: str) -> str:
    """
    Hache un mot de passe avec werkzeug.security
    
    Args:
        password: Mot de passe en clair à hacher
        
    Returns:
        str: Hash du mot de passe (pbkdf2:sha256)
        
    Example:
        >>> hashed = hash_password('mon_mot_de_passe')
        >>> print(hashed)
        'pbkdf2:sha256:260000$...'
    """
    return generate_password_hash(password, method='pbkdf2:sha256')


def verify_password(password: str, password_hash: str) -> bool:
    """
    Vérifie si un mot de passe correspond à son hash
    
    Args:
        password: Mot de passe en clair à vérifier
        password_hash: Hash du mot de passe à comparer
        
    Returns:
        bool: True si le mot de passe correspond, False sinon
        
    Example:
        >>> hashed = hash_password('mon_mot_de_passe')
        >>> verify_password('mon_mot_de_passe', hashed)
        True
        >>> verify_password('mauvais_mot_de_passe', hashed)
        False
    """
    return check_password_hash(password_hash, password)


# ============================================================================
# Utilitaires de validation
# ============================================================================

def validate_email(email: str) -> bool:
    """
    Valide le format d'une adresse email
    
    Args:
        email: Adresse email à valider
        
    Returns:
        bool: True si valide, False sinon
    """
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_password(password: str, min_length: int = 6) -> tuple[bool, str]:
    """
    Valide la force d'un mot de passe
    
    Args:
        password: Mot de passe à valider
        min_length: Longueur minimale requise
        
    Returns:
        tuple[bool, str]: (True/False, message d'erreur ou '')
    """
    if not password:
        return False, "Le mot de passe est requis"
    
    if len(password) < min_length:
        return False, f"Le mot de passe doit contenir au moins {min_length} caractères"
    
    # Optionnel: ajouter des règles supplémentaires
    # - au moins une majuscule
    # - au moins un chiffre
    # - au moins un caractère spécial
    
    return True, ""


def validate_required_fields(data: dict, required_fields: list) -> tuple[bool, str]:
    """
    Vérifie que tous les champs requis sont présents dans les données
    
    Args:
        data: Dictionnaire de données à valider
        required_fields: Liste des champs requis
        
    Returns:
        tuple[bool, str]: (True/False, message d'erreur ou '')
    """
    missing_fields = [field for field in required_fields if not data.get(field)]
    
    if missing_fields:
        return False, f"Champs manquants: {', '.join(missing_fields)}"
    
    return True, ""


# ============================================================================
# Utilitaires de réponse
# ============================================================================

def success_response(message: str, data: dict = None, status_code: int = 200):
    """
    Génère une réponse de succès standardisée
    
    Args:
        message: Message de succès
        data: Données à retourner (optionnel)
        status_code: Code HTTP de la réponse
        
    Returns:
        tuple: (response, status_code)
    """
    response = {
        'status': 'success',
        'message': message,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    if data:
        response['data'] = data
    
    return jsonify(response), status_code


def error_response(message: str, status_code: int = 400, errors: dict = None):
    """
    Génère une réponse d'erreur standardisée
    
    Args:
        message: Message d'erreur
        status_code: Code HTTP de la réponse
        errors: Détails des erreurs (optionnel)
        
    Returns:
        tuple: (response, status_code)
    """
    response = {
        'status': 'error',
        'message': message,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    if errors:
        response['errors'] = errors
    
    return jsonify(response), status_code


# ============================================================================
# Utilitaires de logging
# ============================================================================

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configure un logger pour l'application
    
    Args:
        name: Nom du logger
        level: Niveau de logging
        
    Returns:
        logging.Logger: Logger configuré
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Éviter les doublons de handlers
    if not logger.handlers:
        # Handler console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # Format
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger


# ============================================================================
# Utilitaires de pagination
# ============================================================================

def paginate(query, page: int = 1, per_page: int = 20):
    """
    Pagine une requête SQLAlchemy
    
    Args:
        query: Requête SQLAlchemy
        page: Numéro de page (commence à 1)
        per_page: Nombre d'éléments par page
        
    Returns:
        dict: Dictionnaire contenant les résultats paginés et les métadonnées
    """
    # Calculer l'offset
    offset = (page - 1) * per_page
    
    # Compter le total
    total = query.count()
    
    # Récupérer les éléments de la page
    items = query.limit(per_page).offset(offset).all()
    
    # Calculer le nombre total de pages
    total_pages = (total + per_page - 1) // per_page
    
    return {
        'items': items,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
    }
