from flask import Blueprint, request, jsonify, current_app
from extensions import db
from models.model import Brigade

brigade_bp = Blueprint('brigade_bp', __name__)


@brigade_bp.route('/brigades', methods=['GET'])
def get_brigades():
    """Liste toutes les brigades"""
    try:
        brigades = Brigade.query.order_by(Brigade.libelle).all()
        return jsonify({
            'success': True,
            'count': len(brigades),
            'data': [{
                'id': b.id,
                'libelle': b.libelle,
                'date_creation': b.date_creation.isoformat() if b.date_creation else None
            } for b in brigades]
        })
    except Exception as e:
        current_app.logger.error(f"Erreur récupération brigades: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@brigade_bp.route('/brigades/<int:brigade_id>', methods=['GET'])
def get_brigade(brigade_id):
    """Récupère une brigade par son ID"""
    try:
        brigade = Brigade.query.get_or_404(brigade_id)
        return jsonify({
            'success': True,
            'data': {
                'id': brigade.id,
                'libelle': brigade.libelle,
                'date_creation': brigade.date_creation.isoformat() if brigade.date_creation else None
            }
        })
    except Exception as e:
        current_app.logger.error(f"Erreur récupération brigade {brigade_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@brigade_bp.route('/brigades', methods=['POST'])
def create_brigade():
    """Crée une nouvelle brigade"""
    try:
        data = request.get_json() if request.is_json else request.form.to_dict()
        libelle = data.get('libelle', '').strip()

        if not libelle:
            return jsonify({'success': False, 'message': 'Le libellé est obligatoire'}), 400

        if Brigade.query.filter_by(libelle=libelle).first():
            return jsonify({'success': False, 'message': f'La brigade "{libelle}" existe déjà'}), 409

        brigade = Brigade(libelle=libelle)
        db.session.add(brigade)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Brigade créée avec succès',
            'data': {
                'id': brigade.id,
                'libelle': brigade.libelle,
                'date_creation': brigade.date_creation.isoformat() if brigade.date_creation else None
            }
        }), 201
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur création brigade: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@brigade_bp.route('/brigades/<int:brigade_id>', methods=['PUT'])
def update_brigade(brigade_id):
    """Met à jour une brigade"""
    try:
        brigade = Brigade.query.get_or_404(brigade_id)
        data = request.get_json() if request.is_json else request.form.to_dict()
        libelle = data.get('libelle', '').strip()

        if not libelle:
            return jsonify({'success': False, 'message': 'Le libellé est obligatoire'}), 400

        existing = Brigade.query.filter_by(libelle=libelle).first()
        if existing and existing.id != brigade_id:
            return jsonify({'success': False, 'message': f'La brigade "{libelle}" existe déjà'}), 409

        brigade.libelle = libelle
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Brigade mise à jour',
            'data': {
                'id': brigade.id,
                'libelle': brigade.libelle,
                'date_creation': brigade.date_creation.isoformat() if brigade.date_creation else None
            }
        })
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur mise à jour brigade {brigade_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@brigade_bp.route('/brigades/<int:brigade_id>', methods=['DELETE'])
def delete_brigade(brigade_id):
    """Supprime une brigade"""
    try:
        brigade = Brigade.query.get_or_404(brigade_id)
        db.session.delete(brigade)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': f'Brigade "{brigade.libelle}" supprimée'
        })
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur suppression brigade {brigade_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500
