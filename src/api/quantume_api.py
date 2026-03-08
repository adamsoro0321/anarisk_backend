from flask import Blueprint, request, jsonify, current_app
from extensions import db
from models.model import Quantume

quantume_bp = Blueprint('quantume_bp', __name__)


@quantume_bp.route('/quantumes', methods=['GET'])
def get_quantumes():
    """Liste tous les quantumes"""
    try:
        quantumes = Quantume.query.order_by(Quantume.libelle).all()
        return jsonify({
            'success': True,
            'count': len(quantumes),
            'data': [{
                'id': q.id,
                'libelle': q.libelle,
                'date_creation': q.date_creation.isoformat() if q.date_creation else None
            } for q in quantumes]
        })
    except Exception as e:
        current_app.logger.error(f"Erreur récupération quantumes: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@quantume_bp.route('/quantumes/<int:quantume_id>', methods=['GET'])
def get_quantume(quantume_id):
    """Récupère un quantume par son ID"""
    try:
        quantume = Quantume.query.get_or_404(quantume_id)
        return jsonify({
            'success': True,
            'data': {
                'id': quantume.id,
                'libelle': quantume.libelle,
                'date_creation': quantume.date_creation.isoformat() if quantume.date_creation else None
            }
        })
    except Exception as e:
        current_app.logger.error(f"Erreur récupération quantume {quantume_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@quantume_bp.route('/quantumes', methods=['POST'])
def create_quantume():
    """Crée un nouveau quantume"""
    try:
        data = request.get_json() if request.is_json else request.form.to_dict()
        libelle = data.get('libelle', '').strip()

        if not libelle:
            return jsonify({'success': False, 'message': 'Le libellé est obligatoire'}), 400

        if Quantume.query.filter_by(libelle=libelle).first():
            return jsonify({'success': False, 'message': f'Le quantum "{libelle}" existe déjà'}), 409

        quantume = Quantume(libelle=libelle)
        db.session.add(quantume)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Quantum créé avec succès',
            'data': {
                'id': quantume.id,
                'libelle': quantume.libelle,
                'date_creation': quantume.date_creation.isoformat() if quantume.date_creation else None
            }
        }), 201
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur création quantume: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@quantume_bp.route('/quantumes/<int:quantume_id>', methods=['PUT'])
def update_quantume(quantume_id):
    """Met à jour un quantume"""
    try:
        quantume = Quantume.query.get_or_404(quantume_id)
        data = request.get_json() if request.is_json else request.form.to_dict()
        libelle = data.get('libelle', '').strip()

        if not libelle:
            return jsonify({'success': False, 'message': 'Le libellé est obligatoire'}), 400

        existing = Quantume.query.filter_by(libelle=libelle).first()
        if existing and existing.id != quantume_id:
            return jsonify({'success': False, 'message': f'Le quantum "{libelle}" existe déjà'}), 409

        quantume.libelle = libelle
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Quantum mis à jour',
            'data': {
                'id': quantume.id,
                'libelle': quantume.libelle,
                'date_creation': quantume.date_creation.isoformat() if quantume.date_creation else None
            }
        })
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur mise à jour quantume {quantume_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@quantume_bp.route('/quantumes/<int:quantume_id>', methods=['DELETE'])
def delete_quantume(quantume_id):
    """Supprime un quantume"""
    try:
        quantume = Quantume.query.get_or_404(quantume_id)
        db.session.delete(quantume)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': f'Quantum "{quantume.libelle}" supprimé'
        })
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur suppression quantume {quantume_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500
