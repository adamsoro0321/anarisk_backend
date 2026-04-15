"""
Blueprint pour l'authentification
Gère les endpoints de login, logout et register
"""

from flask import Blueprint, request, jsonify
from models import User, Role
from extensions import db
from api_utils.utils import (
    generate_token, 
    token_required
)

auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['POST'])
def login():
    """
    Endpoint de connexion
    
    Body JSON attendu:
    {
        "email": "user@example.com",
        "password": "password123"
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'message': 'Données JSON requises',
                'error_code': 'JSON_REQUIRED'
            }), 400
            
        email = data.get('email')
        password = data.get('password')
        # Validation des champs requis
        if not email or not password:
            return jsonify({
                'success': False,
                'message': 'Email et mot de passe requis',
                'error_code': 'MISSING_CREDENTIALS'
            }), 400
            
        # Rechercher l'utilisateur par email
        user = User.query.filter_by(email=email).first()
        if not user:
            return jsonify({
                'success': False,
                'message': 'Identifiants incorrects',
                'error_code': 'INVALID_CREDENTIALS'
            }), 401
            
        # Vérifier le statut de l'utilisateur
        if user.status != 'active':
            return jsonify({
                'success': False,
                'message': 'Compte désactivé. Contactez l\'administrateur.',
                'error_code': 'ACCOUNT_DISABLED'
            }), 403
            
        # Vérifier le mot de passe
        if not user.check_password(password):
            return jsonify({
                'success': False,
                'message': 'Identifiants incorrects',
                'error_code': 'INVALID_CREDENTIALS'
            }), 401
            
        # Récupérer le rôle principal de l'utilisateur
        user_role = user.roles[0].intitule if user.roles else 'user'
        
        # Générer le token JWT avec ur et brigade
        token = generate_token(user.id, user.email, user_role, user.ur, user.brigade)
        
        return jsonify({
            'success': True,
            'message': 'Connexion réussie',
            'token': token,
            'user': {
                'id': user.id,
                'email': user.email,
                'nom': user.nom,
                'prenom': user.prenom,
                'role': user_role,
                'roles': [role.intitule for role in user.roles],
                'ur': user.ur,
                'brigade': user.brigade
            }
        }), 200
            
    except Exception as e:
        print(f"Error during login: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@auth_bp.route('/logout', methods=['POST'])
@token_required
def logout():
    """
    Endpoint de déconnexion
    Note: Avec JWT, la déconnexion est gérée côté client en supprimant le token
    """
    return jsonify({
        'success': True,
        'message': 'Déconnexion réussie'
    }), 200


@auth_bp.route('/register', methods=['POST'])
def register():
    """
    Endpoint d'inscription
    
    Body JSON attendu:
    {
        "email": "user@example.com",
        "password": "password123",
        "nom": "Nom",
        "prenom": "Prenom",
        "role": "user",  // optionnel, défaut: "user"
        "ur": "DGE",  // optionnel, Unité de Renseignement
        "brigade": "BV1_DGE"  // optionnel, Brigade d'affectation
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'message': 'Données JSON requises',
                'error_code': 'JSON_REQUIRED'
            }), 400
            
        email = data.get('email')
        password = data.get('password')
        nom = data.get('nom')
        prenom = data.get('prenom')
        role = data.get('role', 'user')
        ur = data.get('ur')
        brigade = data.get('brigade')
        status = data.get('status', 'active')
        
        # Validation des champs requis
        if not email or not password:
            return jsonify({
                'success': False,
                'message': 'Email et mot de passe requis',
                'error_code': 'MISSING_CREDENTIALS'
            }), 400
        
        if not nom or not prenom:
            return jsonify({
                'success': False,
                'message': 'Nom et prénom requis',
                'error_code': 'MISSING_NAME'
            }), 400
            
        if len(password) < 6:
            return jsonify({
                'success': False,
                'message': 'Le mot de passe doit contenir au moins 6 caractères',
                'error_code': 'PASSWORD_TOO_SHORT'
            }), 400
        
        # Vérifier si l'email existe déjà
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({
                'success': False,
                'message': 'Cet email est déjà utilisé',
                'error_code': 'EMAIL_EXISTS'
            }), 409
        
        try:
            # Récupérer ou créer le rôle
            role_obj = Role.query.filter_by(intitule=role).first()
            if not role_obj:
                # Si le rôle n'existe pas, créer un rôle 'user' par défaut
                role_obj = Role.query.filter_by(intitule='user').first()
                if not role_obj:
                    role_obj = Role(intitule='user', description='Utilisateur standard')
                    db.session.add(role_obj)
                    db.session.flush()
                
            # Créer le nouvel utilisateur avec tous les attributs
            new_user = User(
                email=email,
                nom=nom,
                prenom=prenom,
                status=status,
                ur=ur,
                brigade=brigade
            )
            # Le mot de passe est automatiquement hashé via set_password
            new_user.set_password(password)
            new_user.roles.append(role_obj)
            
            db.session.add(new_user)
            db.session.commit()
            
            return jsonify({
                'success': True,
                'message': 'Utilisateur créé avec succès',
                'user': {
                    'id': new_user.id,
                    'email': new_user.email,
                    'nom': new_user.nom,
                    'prenom': new_user.prenom,
                    'role': role_obj.intitule,
                    'roles': [r.intitule for r in new_user.roles],
                    'ur': new_user.ur,
                    'brigade': new_user.brigade,
                    'status': new_user.status
                }
            }), 201
            
        except Exception as e:
            db.session.rollback()
            print(f"Error creating user: {str(e)}")
            return jsonify({
                'success': False,
                'message': f'Erreur lors de la création de l\'utilisateur: {str(e)}',
                'error_code': 'USER_CREATION_ERROR'
            }), 500
            
    except Exception as e:
        print(f"Error in register: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@auth_bp.route('/me', methods=['GET'])
@token_required
def get_current_user():
    """
    Retourne les informations de l'utilisateur connecté
    """
    try:
        # Récupérer les informations depuis request (ajoutées par @token_required)
        user_id = request.user_id
        
        # Charger l'utilisateur depuis la base de données pour avoir toutes les infos
        user = User.query.get(user_id)
        if not user:
            return jsonify({
                'success': False,
                'message': 'Utilisateur non trouvé',
                'error_code': 'USER_NOT_FOUND'
            }), 404
        
        return jsonify({
            'success': True,
            'user': {
                'id': user.id,
                'email': user.email,
                'nom': user.nom,
                'prenom': user.prenom,
                'role': user.roles[0].intitule if user.roles else None,
                'roles': [role.intitule for role in user.roles],
                'ur': user.ur,
                'brigade': user.brigade,
                'status': user.status
            }
        }), 200
        
    except Exception as e:
        print(f"Error in get_current_user: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500
