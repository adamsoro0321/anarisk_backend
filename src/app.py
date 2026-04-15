"""
ANARISK Backend API
Direction Générale des Impôts - Burkina Faso
Système d'Analyse des Risques Fiscaux
"""

import os
import sys
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from dotenv import load_dotenv
from celery.utils.log import get_task_logger
import redis

# Ajouter le dossier src au path
sys.path.append(os.path.dirname(__file__))

from core.data_loader import DataLoader
from core.risk_compute import RiskComputer
from core.fiches_generator import FichesGenerator
from utils.util import get_latest_risk_file
from db.ods import connectionOds
from celery import Celery
from flask_sqlalchemy import SQLAlchemy
# Import du package API
from api import register_blueprints 
from extensions import db as f_db
from core import sql_ifu
from sqlalchemy import create_engine
from data_extractor import DataExtractor
from flask_apscheduler import APScheduler
from config import Config
import uuid
from task import generate_fiches, run_risk_analysis
from task_manager import task_manager
from waitress import serve
load_dotenv()

# ============================================================================
# Configuration de l'application Flask 
logger = get_task_logger(__name__)
database_url = os.getenv("DATABASE_URL")
DATA_DIR ="../data"
OUTPUT_DIR ="../output"
PROGRAMME_DIR ="../programmes"
FICHES_DIR ="../fiches"
DOCS_DIR ="../docs"
RISK_CONTRIBUABLE_DIR = "../data/risk_contribuables"

IFU_DB_URL = os.getenv("IFU_DB_URL")
ODS_DB_URL = os.getenv("ODS_DB_URL")


def _find_file_by_stem(directory: str, stem: str) -> bool:
    """Vérifie si un fichier ou dossier dont le nom (sans extension) correspond à stem existe dans directory."""
    import pathlib
    d = pathlib.Path(os.path.dirname(__file__)) / directory
    if not d.exists():
        return False
    stem_lower = stem.lower()
    for entry in d.iterdir():
        entry_stem = entry.stem if entry.is_file() else entry.name
        if entry_stem.lower() == stem_lower:
            return True
    return False


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'anarisk-dgi-burkina-faso-secret-key-2025')
    app.config['JWT_EXPIRATION_HOURS'] = int(os.getenv('JWT_EXPIRATION_HOURS', 24))
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url 
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    # Configuration CORS pour permettre les requêtes du frontend
    CORS(app, resources={
        r"/*": {
            "origins": "*",
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })

    # Initialiser les extensions avec l'app
    f_db.init_app(app)

    return app

def create_celery_app(app):
    celery_app = Celery(
        app.import_name,
        broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
        backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    )
    celery_app.conf.update(
        task_track_started=True,
        result_extended=True
    )
    return celery_app


app = create_app()
app.config.from_object(Config)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# Ajouter une tâche de nettoyage automatique des anciennes tâches (toutes les heures)
def cleanup_old_tasks():
    """Nettoie les tâches terminées anciennes"""
    try:
        count = task_manager.cleanup_old_tasks()
        print(f"🧹 Nettoyage automatique : {count} tâche(s) supprimée(s)")
    except Exception as e:
        print(f"⚠️ Erreur lors du nettoyage automatique : {e}")

scheduler.add_job(
    id='cleanup_old_tasks',
    func=cleanup_old_tasks,
    trigger='interval',
    hours=1  # Toutes les heures
)


celery_app = create_celery_app(app)
celery_app.conf.enable_utc = True
celery_app.conf.update(
    enable_utc=True,
    timezone='UTC',)
# Enregistrement des blueprints API
register_blueprints(app)

# Init variables pour Celery
oracle_engine = connectionOds()

# Client Redis pour le verrou de tâche
redis_client = redis.Redis.from_url(
    os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
)
RISK_ANALYSIS_LOCK_KEY = 'anarisk:run_risk_analysis:lock'
FICHES_LOCK_KEY        = 'anarisk:run_generate_fiches:lock'

# ============================================================================
# Routes de base
# ============================================================================

@app.route('/')
def index():
    """Route racine - Health check"""
    return jsonify({
        'status': 'ok',
        'message': 'ANARISK API - Direction Générale des Impôts - Burkina Faso',
        'version': '1.0.0',
        'endpoints': {
            'auth': '/api/v1/login, /api/v1/logout, /api/v1/register, /api/v1/me',
            'risk_data': '/api/v1/risk-data',
            'stats': '/api/v1/stats, /api/v1/stats/summary, /api/v1/stats/top-risks',
            'analysis': '/api/v1/run, /api/v1/run/status/<task_id>'
        }
    })


@app.route('/api/v1/generate_preliste', methods=['POST'])
def run_analysis():
    """
    genere le preliste
    Lance l'analyse des risques en arrière-plan via Celery.
    Retourne immédiatement un task_id pour suivre la progression.
    Vérifie d'abord qu'aucune tâche identique n'est déjà en cours.
    """
    try:
        quantume = request.args.get("quantume")
        if not quantume:
            return jsonify({
                'success': False,
                'message': 'Veuillez choisir le quantum !'
            }), 400

        #task = run_risk_analysis(quantume)
        job_id = str(uuid.uuid4())
        scheduler.add_job(
            id=job_id, 
            func=run_risk_analysis, 
            args=[quantume, job_id],  # Passer le task_id comme deuxième argument
            trigger='date' 
            )
        return jsonify({
            'success': True,
            'message': 'Analyse des risques lancée',
            'task_id': job_id,
            'status_url': f'/api/v1/run/status/{job_id}'
        }), 202
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors du lancement de l\'analyse',
            'error': str(e)
        }), 500


@app.route('/api/v1/run/status/<task_id>', methods=['GET'])
def run_status(task_id):
    """
    Retourne le statut et la progression d'une tâche d'analyse.

    États possibles : PENDING | RUNNING | SUCCESS | FAILURE
    """
    try:
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                'success': False,
                'task_id': task_id,
                'status': 'NOT_FOUND',
                'message': 'Tâche introuvable'
            }), 404
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'status': task.status.value,
            'progress': task.progress,
            'current_step': task.current_step,
            'start_time': task.start_time.isoformat() if task.start_time else None,
            'end_time': task.end_time.isoformat() if task.end_time else None,
            'result': task.result,
            'error': task.error
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération du statut',
            'error': str(e)
        }), 500


@app.route('/api/v1/run/tasks', methods=['GET'])
def get_run_tasks():
    """
    Endpoint de compatibilité : retourne les tâches actives.
    Alias pour /api/v1/tasks/active
    """
    try:
        tasks = task_manager.get_active_tasks()
        
        # Trier par date de début
        tasks.sort(key=lambda t: t.start_time or t.task_id, reverse=True)
        
        # Format compatible avec l'ancien frontend
        tasks_data = []
        for task in tasks:
            tasks_data.append({
                'task_id': task.task_id,
                'name': task.task_name,
                'state': task.status.value,
                'worker': None,
                'time_start': task.start_time.timestamp() if task.start_time else None
            })
        
        return jsonify({
            'success': True,
            'count': len(tasks_data),
            'data': tasks_data
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des tâches',
            'error': str(e)
        }), 500




@app.route("/api/v1/generate-fiches", methods=["POST"])
def generate_fiches_route():
    try:
        quantume = request.args.get("quantume")
        if not quantume:
            return jsonify({'success': False, 'message': 'Veuillez choisir le quantum !'}), 400

        #verifier si les fichiers suivants existe
        risk_data_path = f"../data/risk_contribuables/{quantume}.csv"
        quantume_path  = f"../programmes/{quantume}.xlsx"

        if not os.path.exists(risk_data_path):
            return jsonify({'success': False, 'message': f'Fichier du risque du quantum « {quantume} » introuvable !'}), 404
        if not os.path.exists(quantume_path):
            return jsonify({'success': False, 'message': f'Fichier du programme du quantum « {quantume} » introuvable !'}), 404

        job_id = str(uuid.uuid4())
        #task = generate_fiches(quantume)
        scheduler.add_job(
            id=job_id, 
            func=generate_fiches, 
            args=[quantume, job_id],  # Passer le task_id comme deuxième argument
            trigger='date' # S'exécute une seule fois
            )
        
        return jsonify({
            'success': True,
            'message': f'Génération des fiches lancée pour le quantum « {quantume} »',
            'task_id': job_id,
            'status_url': f'/api/v1/run/status/{job_id}'
        }), 202

    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors du lancement de la génération des fiches',
            'error': str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'message': 'Ressource non trouvée',
        'error_code': 'NOT_FOUND'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'message': 'Erreur interne du serveur',
        'error_code': 'INTERNAL_SERVER_ERROR'
    }), 500


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'message': 'Méthode non autorisée',
        'error_code': 'METHOD_NOT_ALLOWED'
    }), 405

if __name__ == '__main__':
    # Configuration du serveur
    host = os.getenv('FLASK_HOST', '127.0.0.1')
    port = int(os.getenv('FLASK_PORT', 5000))
    print(f"🚀 ANARISK API démarré sur http://{host}:{port}")
    with app.app_context():
        # Corriger la contrainte unique sur programmes.IFU si nécessaire
        from sqlalchemy import inspect as sa_inspect, text
        insp = sa_inspect(f_db.engine)
        if insp.has_table('programmes'):
            indexes = insp.get_indexes('programmes')
            unique_on_ifu = any(
                idx.get('unique') and 'IFU' in idx.get('column_names', []) and len(idx.get('column_names', [])) == 1
                for idx in indexes
            )
            if unique_on_ifu:
                print("⚠️  Contrainte unique détectée sur programmes.IFU — suppression de la table pour recréation...")
                f_db.metadata.tables['programmes'].drop(f_db.engine)
                print("✓ Table programmes supprimée")
        f_db.create_all()
   # app.run(host=host, port=port, debug=True)
    #serve(app, host='0.0.0.0', port=port, threads=1) #WAITRESS!
