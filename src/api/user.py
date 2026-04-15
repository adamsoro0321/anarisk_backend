from flask import Blueprint, jsonify, request, current_app
import pandas as pd
import os
from models import User, Role
from extensions import db
users_bp = Blueprint('users', __name__)

@users_bp.route('/users', methods=['GET'])
def get_users():
    """
    Endpoint pour récupérer la liste des utilisateurs avec pagination.
    
    Query Parameters:
        - page (int): Numéro de la page (défaut: 1)
        - per_page (int): Nombre d'éléments par page (défaut: 10, max: 100)"""
    try:
        page = request.args.get('page', default=1, type=int)
        per_page = request.args.get('per_page', default=10, type=int)
        per_page = min(per_page, 100)  # Limite à 100 par page
        
        users_query = User.query.order_by(User.id)
        pagination = users_query.paginate(page=page, per_page=per_page, error_out=False)
        
        users = [{
            'id': user.id,
            'nom': user.nom,
            'prenom': user.prenom,
            'email': user.email,
            'status': user.status,
            'ur': user.ur,
            'brigade': user.brigade,
            'date_creation': user.date_creation.isoformat() if user.date_creation else None,
            'date_modification': user.date_modification.isoformat() if user.date_modification else None,
            'roles': [role.intitule for role in user.roles]
        } for user in pagination.items]
        
        return jsonify({
            'success': True,
            'data': users,
            'pagination': {
                'total': pagination.total,
                'pages': pagination.pages,
                'current_page': pagination.page,
                'per_page': pagination.per_page
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@users_bp.route('/users', methods=['POST'])
def create_user():
    """
    Endpoint pour créer un nouvel utilisateur.
    
    JSON Body:
        - nom (str): Nom de l'utilisateur
        - prenom (str): Prénom de l'utilisateur
        - email (str): Email de l'utilisateur
        - password (str): Mot de passe de l'utilisateur
        - role (str): Rôle à assigner à l'utilisateur (ex: 'admin', 'analyste', etc.)
        - ur (str, optional): Unité de recouvrement
        - brigade (str, optional): Brigade d'affectation
    """
    try:
        data = request.get_json()
        nom = data.get('nom')
        prenom = data.get('prenom')
        email = data.get('email')
        password = data.get('password', "password123")  # Mot de passe par défaut si non fourni
        role = data.get('role', 'user')  # Rôle par défaut 'user'
        ur = data.get('ur')
        brigade = data.get('brigade')
        
        if not all([nom, prenom, email, password]):
            return jsonify({
                'success': False,
                'message': 'Tous les champs nom, prenom, email et password sont requis',
                'error_code': 'MISSING_FIELDS'
            }), 400 
        if User.query.filter_by(email=email).first():
            return jsonify({
                'success': False,
                'message': 'Un utilisateur avec cet email existe déjà',
                'error_code': 'EMAIL_ALREADY_EXISTS'
            }), 400
        
        user = User(nom=nom, prenom=prenom, email=email, ur=ur, brigade=brigade)
        user.set_password(password)
        
        role_obj = Role.query.filter_by(intitule=role).first()
        if not role_obj:
            role_obj = Role(intitule=role)
            db.session.add(role_obj)
            db.session.flush()  # Flush pour obtenir l'ID du rôle
        user.roles.append(role_obj)
        
        db.session.add(user)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Utilisateur créé avec succès',
            'data': {
                'id': user.id,
                'nom': user.nom,
                'prenom': user.prenom,
                'email': user.email,
                'ur': user.ur,
                'brigade': user.brigade,
                'roles': [role.intitule for role in user.roles]
            }
        }), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500