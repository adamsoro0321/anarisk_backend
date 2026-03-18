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
from utils.util import get_latest_risk_file
from db.ods import connectionOds
from celery import Celery
from flask_sqlalchemy import SQLAlchemy
# Import du package API
from api import register_blueprints 
from extensions import db as f_db
load_dotenv()

# ============================================================================
# Configuration de l'application Flask 
logger = get_task_logger(__name__)
database_url = os.getenv("DATABASE_URL")

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

# ============================================================================
# Routes de base
# ============================================================================

@celery_app.task(bind=True, name='app.run_risk_analysis')
def run_risk_analysis(self):
    """
    Tâche Celery : extraction, fusion et calcul des indicateurs de risque.
    Étapes :
      1. Connexion Oracle
      2. Extraction & fusion des données (DataLoader)
      3. Calcul des indicateurs de risque (RiskComputer)
      4. Sauvegarde CSV dans data/risk_contribuables/
    Le statut de progression est mis à jour via self.update_state() et peut être
    consulté via GET /api/v1/run/status/<task_id>.
    """
    connect = None
    try:
        # Poser le verrou Redis (TTL 2h max pour éviter un verrou permanent)
        redis_client.set(RISK_ANALYSIS_LOCK_KEY, self.request.id, ex=7200)
        # ── Étape 1 : Connexion Oracle ────────────────────────────────────
        connect = oracle_engine.connect()

        # ── Étape 2 : Extraction & fusion ────────────────────────────────
        loader = DataLoader(oracle_engine)
        merged_data = loader.run_extract_merge()

        if merged_data is None or merged_data.empty:
            raise ValueError('Aucune donnée retournée par DataLoader.run_extract_merge()')

        logger.info(f"Données fusionnées : {len(merged_data)} lignes, {len(merged_data.columns)} colonnes")

        # ── Étape 3 : Calcul des indicateurs ─────────────────────────────
        computer = RiskComputer()
        result = computer.run(data=merged_data)

        if result.get('status') != 'success':
            raise RuntimeError(result.get('message', 'Erreur inconnue dans RiskComputer.run()'))

        summary = {
            'nb_contribuables': result.get('nb_contribuables', 0),
            'nb_indicateurs':   result.get('nb_indicateurs', 0),
            'elapsed_time':     result.get('elapsed_time', 0),
            'file':             result.get('file', ''),
            'generated_at':     datetime.now().isoformat(),
        }
        logger.info(
            f"Analyse terminée — {summary['nb_contribuables']} contribuables, "
            f"{summary['nb_indicateurs']} indicateurs, "
            f"{summary['elapsed_time']}s"
        )
        return {'status': 'done', **summary}

    except Exception as exc:
        logger.exception(f"Échec de run_risk_analysis : {exc}")
        # Forcer Celery à marquer la tâche comme FAILURE avec le message d'erreur
        raise  # Celery enregistre l'exception dans le result backend

    finally:
        # Libérer le verrou Redis
        redis_client.delete(RISK_ANALYSIS_LOCK_KEY)
        if connect is not None:
            try:
                connect.close()
            except Exception:
                pass


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


@app.route('/api/v1/generate_quantume', methods=['POST'])
def run_analysis():
    """
    Lance l'analyse des risques en arrière-plan via Celery.
    Retourne immédiatement un task_id pour suivre la progression.
    Vérifie d'abord qu'aucune tâche identique n'est déjà en cours.
    """
    try:
        # Vérifier si une tâche run_risk_analysis est déjà en cours (verrou Redis)
        existing_task_id = redis_client.get(RISK_ANALYSIS_LOCK_KEY)
        if existing_task_id:
            existing_task_id = existing_task_id.decode()
            return jsonify({
                'success': False,
                'message': 'Une analyse des risques est déjà en cours',
                'task_id': existing_task_id,
                'status_url': f'/api/v1/run/status/{existing_task_id}'
            }), 409

        task = run_risk_analysis.delay()
        # Poser le verrou immédiatement côté API pour éviter les appels concurrents
        redis_client.set(RISK_ANALYSIS_LOCK_KEY, task.id, ex=7200)
        return jsonify({
            'success': True,
            'message': 'Analyse des risques lancée',
            'task_id': task.id,
            'status_url': f'/api/v1/run/status/{task.id}'
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

    États possibles : PENDING | PROGRESS | SUCCESS | FAILURE
    """
    try:
        task = celery_app.AsyncResult(task_id)
        meta = task.info if isinstance(task.info, dict) else {}

        payload = {
            'success': True,
            'task_id': task_id,
            'state': task.state,
        }

        if task.state == 'PENDING':
            payload['status'] = 'En attente de démarrage…'

        elif task.state == 'PROGRESS':
            payload['status']  = meta.get('status', '')
            payload['current'] = meta.get('current', 0)
            payload['total']   = meta.get('total', 1)
            payload['percent'] = round(meta.get('current', 0) / max(meta.get('total', 1), 1) * 100)

        elif task.state == 'SUCCESS':
            payload['status'] = 'Terminé avec succès'
            payload['result'] = task.result

        elif task.state == 'FAILURE':
            payload['success'] = False
            payload['status']  = 'Échec de la tâche'
            payload['error']   = meta.get('exc_message', str(task.result))

        return jsonify(payload), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération du statut',
            'error': str(e)
        }), 500


@app.route('/api/v1/run/tasks', methods=['GET'])
def list_tasks():
    """
    Liste toutes les tâches Celery connues (actives, réservées, planifiées).

    Query params optionnels :
      - state : filtre sur un état spécifique (ACTIVE | RESERVED | SCHEDULED)
      - task_ids : liste d'IDs séparés par virgule pour interroger des tâches précises

    Retourne pour chaque tâche :
      id, name, state, status, args, kwargs, time_start, worker
    """
    try:
        inspect = celery_app.control.inspect(timeout=3.0)

        # Récupérer les tâches depuis les workers
        active_raw    = inspect.active()    or {}
        reserved_raw  = inspect.reserved()  or {}
        scheduled_raw = inspect.scheduled() or {}

        def _normalize(tasks_by_worker: dict, state: str) -> list:
            results = []
            for worker, tasks in tasks_by_worker.items():
                for t in tasks:
                    results.append({
                        'task_id':    t.get('id'),
                        'name':       t.get('name', ''),
                        'state':      state,
                        'worker':     worker,
                        'args':       t.get('args', []),
                        'kwargs':     t.get('kwargs', {}),
                        'time_start': t.get('time_start'),
                    })
            return results

        all_tasks = (
            _normalize(active_raw,    'ACTIVE')
            + _normalize(reserved_raw,  'RESERVED')
            + _normalize(scheduled_raw, 'SCHEDULED')
        )

        # Filtre optionnel par state
        state_filter = request.args.get('state', '').upper()
        if state_filter:
            all_tasks = [t for t in all_tasks if t['state'] == state_filter]

        # Filtre optionnel par IDs explicites (task_ids=id1,id2,...)
        ids_param = request.args.get('task_ids', '').strip()
        if ids_param:
            id_set = {i.strip() for i in ids_param.split(',') if i.strip()}
            # Compléter avec le result backend pour les IDs inconnus des workers
            for task_id in id_set:
                if not any(t['task_id'] == task_id for t in all_tasks):
                    result = celery_app.AsyncResult(task_id)
                    meta   = result.info if isinstance(result.info, dict) else {}
                    all_tasks.append({
                        'task_id':    task_id,
                        'name':       meta.get('name', 'app.run_risk_analysis'),
                        'state':      result.state,
                        'worker':     None,
                        'args':       [],
                        'kwargs':     {},
                        'time_start': None,
                        'meta':       meta,
                    })
            all_tasks = [t for t in all_tasks if t['task_id'] in id_set]

        return jsonify({
            'success': True,
            'count':   len(all_tasks),
            'data':    all_tasks,
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des tâches',
            'error': str(e)
        }), 500


@app.route('/api/v1/run/tasks/<task_id>', methods=['DELETE'])
def revoke_task(task_id):
    """
    Révoque (annule) une tâche Celery en attente ou en cours.

    Body JSON optionnel :
      { "terminate": true }  — force l'arrêt même si la tâche est déjà démarrée
    """
    try:
        body      = request.get_json(silent=True) or {}
        terminate = bool(body.get('terminate', False))

        celery_app.control.revoke(task_id, terminate=terminate, signal='SIGTERM')

        return jsonify({
            'success': True,
            'task_id':   task_id,
            'terminate': terminate,
            'message':   f"Tâche {task_id} révoquée{'  (terminée de force)' if terminate else ''}.",
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la révocation de la tâche',
            'error': str(e)
        }), 500



# ============================================================================

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
    app.run(host=host, port=port, debug=True)
