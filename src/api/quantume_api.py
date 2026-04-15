from flask import Blueprint, request, jsonify, current_app
from extensions import db
from models.model import Quantume
from pathlib import Path
from datetime import datetime
import os

quantume_bp = Blueprint('quantume_bp', __name__)

# Chemins clés relatifs à la racine du projet (anarisk_backend/)
_BASE_DIR      = Path(__file__).parent.parent.parent
_PRELISTE_DIR  = _BASE_DIR / 'data' / 'risk_contribuables'
_PROGRAMME_DIR = _BASE_DIR / 'programmes'
_FICHES_DIR    = _BASE_DIR / 'fiches'


def _fmt_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def _check_preliste(libelle: str) -> dict:
    """Vérifie si un fichier ou dossier pré-liste existe pour ce quantum."""
    if not _PRELISTE_DIR.exists():
        return {'exists': False, 'info': None}
    lib_lower = libelle.lower()
    for entry in _PRELISTE_DIR.iterdir():
        stem = entry.stem if entry.is_file() else entry.name
        if stem.lower() == lib_lower:
            stat = entry.stat()
            return {
                'exists': True,
                'info': {
                    'name': entry.name,
                    'type': 'csv' if entry.is_file() else 'dossier',
                    'size_formatted': _fmt_size(stat.st_size) if entry.is_file() else None,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            }
    return {'exists': False, 'info': None}


def _check_programme(libelle: str) -> dict:
    """Vérifie si un fichier programme (xlsx/xls/csv) existe pour ce quantum."""
    if not _PROGRAMME_DIR.exists():
        return {'exists': False, 'info': None}
    lib_lower = libelle.lower()
    for entry in _PROGRAMME_DIR.iterdir():
        if entry.is_file() and entry.stem.lower() == lib_lower and entry.suffix.lower() in ('.xlsx', '.xls', '.csv'):
            stat = entry.stat()
            return {
                'exists': True,
                'info': {
                    'name': entry.name,
                    'size_formatted': _fmt_size(stat.st_size),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            }
    return {'exists': False, 'info': None}


def _check_fiches(libelle: str) -> dict:
    """Vérifie si un dossier de fiches existe pour ce quantum."""
    if not _FICHES_DIR.exists():
        return {'exists': False, 'info': None}
    lib_lower = libelle.lower()
    for entry in _FICHES_DIR.iterdir():
        if entry.is_dir() and entry.name.lower() == lib_lower:
            nb = sum(1 for f in entry.iterdir() if f.is_file())
            stat = entry.stat()
            return {
                'exists': True,
                'info': {
                    'name': entry.name,
                    'nb_fiches': nb,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                }
            }
    return {'exists': False, 'info': None}


@quantume_bp.route('/quantumes/status', methods=['GET'])
def get_quantumes_status():
    """
    Retourne pour chaque quantume la disponibilité des artefacts :
      - preliste  : data/risk_contribuables/<libelle>.csv  ou dossier <libelle>/
      - programme : programmes/<libelle>.xlsx (ou .xls/.csv)
      - fiches    : fiches/<libelle>/  (dossier)
    """
    try:
        quantumes = Quantume.query.order_by(Quantume.date_creation.desc().nullslast()).all()
        data = []
        for q in quantumes:
            data.append({
                'id':            q.id,
                'libelle':       q.libelle,
                'date_creation': q.date_creation.isoformat() if q.date_creation else None,
                'preliste':      _check_preliste(q.libelle),
                'programme':     _check_programme(q.libelle),
                'fiches':        _check_fiches(q.libelle),
            })
        return jsonify({'success': True, 'count': len(data), 'data': data})
    except Exception as e:
        current_app.logger.error(f"Erreur statut quantumes: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@quantume_bp.route('/quantumes', methods=['GET'])
def get_quantumes():
    """Liste tous les quantumes"""
    try:
        quantumes = Quantume.query.order_by(Quantume.date_creation.desc().nullslast()).all()
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
