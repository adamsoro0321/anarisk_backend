"""
Task Manager - Système de gestion des tâches APScheduler
Direction Générale des Impôts - Burkina Faso

Gère l'état, les logs et la progression des tâches en mémoire.
"""

import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum


class TaskStatus(Enum):
    """États possibles d'une tâche"""
    PENDING = "PENDING"      # Tâche créée, pas encore démarrée
    RUNNING = "RUNNING"      # Tâche en cours d'exécution
    SUCCESS = "SUCCESS"      # Tâche terminée avec succès
    FAILURE = "FAILURE"      # Tâche échouée
    CANCELLED = "CANCELLED"  # Tâche annulée


class TaskInfo:
    """Informations sur une tâche"""
    
    def __init__(self, task_id: str, task_name: str, quantume: Optional[str] = None):
        self.task_id = task_id
        self.task_name = task_name
        self.quantume = quantume
        self.status = TaskStatus.PENDING
        self.logs: List[Dict[str, Any]] = []
        self.progress = 0  # 0-100
        self.current_step = ""
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.metadata: Dict[str, Any] = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convertit les informations de la tâche en dictionnaire"""
        return {
            'task_id': self.task_id,
            'task_name': self.task_name,
            'quantume': self.quantume,
            'status': self.status.value,
            'logs': self.logs,
            'progress': self.progress,
            'current_step': self.current_step,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self._calculate_duration(),
            'result': self.result,
            'error': self.error,
            'metadata': self.metadata
        }
    
    def _calculate_duration(self) -> Optional[float]:
        """Calcule la durée d'exécution en secondes"""
        if not self.start_time:
            return None
        end = self.end_time or datetime.now()
        delta = end - self.start_time
        return round(delta.total_seconds(), 2)
    
    def add_log(self, message: str, level: str = "INFO"):
        """Ajoute un message de log"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        }
        self.logs.append(log_entry)
        
        # Limiter le nombre de logs à 1000 pour éviter une consommation excessive de mémoire
        if len(self.logs) > 1000:
            self.logs = self.logs[-1000:]
    
    def start(self):
        """Marque la tâche comme démarrée"""
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.now()
        self.add_log(f"Tâche '{self.task_name}' démarrée", "INFO")
    
    def update_progress(self, progress: int, step: str = ""):
        """Met à jour la progression de la tâche"""
        self.progress = max(0, min(100, progress))
        if step:
            self.current_step = step
            self.add_log(f"Étape : {step} ({progress}%)", "INFO")
    
    def complete(self, result: Optional[Dict[str, Any]] = None):
        """Marque la tâche comme terminée avec succès"""
        self.status = TaskStatus.SUCCESS
        self.end_time = datetime.now()
        self.progress = 100
        self.result = result
        self.add_log(f"Tâche '{self.task_name}' terminée avec succès", "SUCCESS")
    
    def fail(self, error: str):
        """Marque la tâche comme échouée"""
        self.status = TaskStatus.FAILURE
        self.end_time = datetime.now()
        self.error = error
        self.add_log(f"Erreur : {error}", "ERROR")
    
    def cancel(self):
        """Annule la tâche"""
        self.status = TaskStatus.CANCELLED
        self.end_time = datetime.now()
        self.add_log(f"Tâche '{self.task_name}' annulée", "WARNING")


class TaskManager:
    """
    Gestionnaire de tâches singleton.
    Gère l'état et les logs des tâches APScheduler en mémoire.
    Thread-safe grâce à l'utilisation de locks.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._tasks: Dict[str, TaskInfo] = {}
        self._lock = threading.Lock()
        self._max_completed_tasks = 50  # Nombre maximum de tâches terminées à conserver
        self._cleanup_after_hours = 24  # Supprimer les tâches terminées après X heures
        self._initialized = True
    
    def create_task(self, task_id: str, task_name: str, quantume: Optional[str] = None) -> TaskInfo:
        """Crée une nouvelle tâche"""
        with self._lock:
            task = TaskInfo(task_id, task_name, quantume)
            self._tasks[task_id] = task
            return task
    
    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        """Récupère une tâche par son ID"""
        with self._lock:
            return self._tasks.get(task_id)
    
    def get_all_tasks(self, include_completed: bool = True) -> List[TaskInfo]:
        """Récupère toutes les tâches"""
        with self._lock:
            tasks = list(self._tasks.values())
            if not include_completed:
                tasks = [t for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.RUNNING)]
            return tasks
    
    def get_active_tasks(self) -> List[TaskInfo]:
        """Récupère uniquement les tâches actives (PENDING ou RUNNING)"""
        return self.get_all_tasks(include_completed=False)
    
    def delete_task(self, task_id: str) -> bool:
        """Supprime une tâche"""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            return False
    
    def cleanup_old_tasks(self):
        """Nettoie les tâches terminées anciennes"""
        with self._lock:
            now = datetime.now()
            cutoff_time = now - timedelta(hours=self._cleanup_after_hours)
            
            # Identifier les tâches à supprimer
            tasks_to_remove = []
            for task_id, task in self._tasks.items():
                if task.status in (TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED):
                    if task.end_time and task.end_time < cutoff_time:
                        tasks_to_remove.append(task_id)
            
            # Supprimer les tâches
            for task_id in tasks_to_remove:
                del self._tasks[task_id]
            
            # Limiter le nombre total de tâches terminées
            completed_tasks = [
                (tid, t) for tid, t in self._tasks.items()
                if t.status in (TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED)
            ]
            
            if len(completed_tasks) > self._max_completed_tasks:
                # Trier par date de fin (les plus anciennes en premier)
                completed_tasks.sort(key=lambda x: x[1].end_time or datetime.min)
                # Supprimer les plus anciennes
                to_remove = len(completed_tasks) - self._max_completed_tasks
                for i in range(to_remove):
                    del self._tasks[completed_tasks[i][0]]
            
            return len(tasks_to_remove)
    
    def get_task_count(self) -> Dict[str, int]:
        """Retourne le nombre de tâches par statut"""
        with self._lock:
            counts = {
                'total': len(self._tasks),
                'pending': 0,
                'running': 0,
                'success': 0,
                'failure': 0,
                'cancelled': 0
            }
            for task in self._tasks.values():
                status_key = task.status.value.lower()
                if status_key in counts:
                    counts[status_key] += 1
            return counts
    
    def clear_all_tasks(self):
        """Supprime toutes les tâches (à utiliser avec précaution)"""
        with self._lock:
            self._tasks.clear()


# Instance singleton globale
task_manager = TaskManager()
