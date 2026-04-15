"""
API Routes pour la gestion des tâches
Direction Générale des Impôts - Burkina Faso
"""

from flask import Blueprint, jsonify, request
from task_manager import task_manager, TaskStatus

tasks_bp = Blueprint('tasks', __name__, url_prefix='/api/v1/tasks')


@tasks_bp.route('', methods=['GET'])
def get_all_tasks():
    """
    Récupère la liste de toutes les tâches.
    
    Query params:
        - include_completed: true/false (défaut: true)
        - limit: nombre max de tâches à retourner
    """
    try:
        include_completed = request.args.get('include_completed', 'true').lower() == 'true'
        limit = request.args.get('limit', type=int)
        
        tasks = task_manager.get_all_tasks(include_completed=include_completed)
        
        # Trier par date de début (plus récentes en premier)
        tasks.sort(key=lambda t: t.start_time or t.task_id, reverse=True)
        
        # Limiter si nécessaire
        if limit and limit > 0:
            tasks = tasks[:limit]
        
        tasks_data = [task.to_dict() for task in tasks]
        
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


@tasks_bp.route('/active', methods=['GET'])
def get_active_tasks():
    """
    Récupère uniquement les tâches actives (PENDING ou RUNNING).
    """
    try:
        tasks = task_manager.get_active_tasks()
        
        # Trier par date de début
        tasks.sort(key=lambda t: t.start_time or t.task_id, reverse=True)
        
        tasks_data = [task.to_dict() for task in tasks]
        
        return jsonify({
            'success': True,
            'count': len(tasks_data),
            'data': tasks_data
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des tâches actives',
            'error': str(e)
        }), 500


@tasks_bp.route('/stats', methods=['GET'])
def get_tasks_stats():
    """
    Récupère les statistiques sur les tâches.
    """
    try:
        stats = task_manager.get_task_count()
        
        return jsonify({
            'success': True,
            'data': stats
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des statistiques',
            'error': str(e)
        }), 500


@tasks_bp.route('/<task_id>', methods=['GET'])
def get_task_details(task_id):
    """
    Récupère les détails complets d'une tâche.
    """
    try:
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                'success': False,
                'message': 'Tâche introuvable',
                'task_id': task_id
            }), 404
        
        return jsonify({
            'success': True,
            'data': task.to_dict()
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération de la tâche',
            'error': str(e)
        }), 500


@tasks_bp.route('/<task_id>/logs', methods=['GET'])
def get_task_logs(task_id):
    """
    Récupère les logs d'une tâche.
    
    Query params:
        - limit: nombre max de logs à retourner (défaut: tous)
        - level: filtrer par niveau (INFO, WARNING, ERROR, SUCCESS)
        - since: timestamp ISO - retourner uniquement les logs depuis cette date
    """
    try:
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                'success': False,
                'message': 'Tâche introuvable',
                'task_id': task_id
            }), 404
        
        logs = task.logs
        
        # Filtrer par niveau si demandé
        level_filter = request.args.get('level')
        if level_filter:
            logs = [log for log in logs if log['level'] == level_filter.upper()]
        
        # Filtrer par date si demandé
        since = request.args.get('since')
        if since:
            logs = [log for log in logs if log['timestamp'] >= since]
        
        # Limiter le nombre de logs si demandé
        limit = request.args.get('limit', type=int)
        if limit and limit > 0:
            logs = logs[-limit:]  # Prendre les N derniers logs
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'count': len(logs),
            'logs': logs
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération des logs',
            'error': str(e)
        }), 500


@tasks_bp.route('/<task_id>/status', methods=['GET'])
def get_task_status(task_id):
    """
    Récupère uniquement le statut d'une tâche (léger, pour polling fréquent).
    """
    try:
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                'success': False,
                'message': 'Tâche introuvable',
                'task_id': task_id
            }), 404
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'status': task.status.value,
            'progress': task.progress,
            'current_step': task.current_step,
            'error': task.error
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la récupération du statut',
            'error': str(e)
        }), 500


@tasks_bp.route('/<task_id>', methods=['DELETE'])
def delete_task(task_id):
    """
    Supprime une tâche (uniquement si terminée).
    """
    try:
        task = task_manager.get_task(task_id)
        
        if not task:
            return jsonify({
                'success': False,
                'message': 'Tâche introuvable',
                'task_id': task_id
            }), 404
        
        # Vérifier que la tâche est terminée
        if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return jsonify({
                'success': False,
                'message': 'Impossible de supprimer une tâche en cours',
                'task_id': task_id,
                'status': task.status.value
            }), 400
        
        # Supprimer la tâche
        deleted = task_manager.delete_task(task_id)
        
        if deleted:
            return jsonify({
                'success': True,
                'message': 'Tâche supprimée avec succès',
                'task_id': task_id
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Erreur lors de la suppression',
                'task_id': task_id
            }), 500
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors de la suppression de la tâche',
            'error': str(e)
        }), 500


@tasks_bp.route('/cleanup', methods=['POST'])
def cleanup_tasks():
    """
    Nettoie les tâches terminées anciennes.
    """
    try:
        count = task_manager.cleanup_old_tasks()
        
        return jsonify({
            'success': True,
            'message': f'{count} tâche(s) nettoyée(s)',
            'count': count
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': 'Erreur lors du nettoyage',
            'error': str(e)
        }), 500
