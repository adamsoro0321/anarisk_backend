"""
Task Logger - Système de logging personnalisé pour les tâches APScheduler
Direction Générale des Impôts - Burkina Faso

Capture les logs des tâches et les envoie au TaskManager pour un suivi en temps réel.
"""

import logging
import sys
from contextlib import contextmanager
from typing import Optional
from task_manager import task_manager, TaskInfo


class TaskLogHandler(logging.Handler):
    """
    Handler de logging personnalisé qui envoie les logs vers le TaskManager.
    Permet de capturer tous les logs émis pendant l'exécution d'une tâche.
    """
    
    def __init__(self, task_id: str):
        super().__init__()
        self.task_id = task_id
        
        # Formatteur de logs
        formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )
        self.setFormatter(formatter)
    
    def emit(self, record: logging.LogRecord):
        """
        Appelé automatiquement pour chaque log émis.
        Envoie le log au TaskManager.
        """
        try:
            task = task_manager.get_task(self.task_id)
            if task:
                # Formater le message
                msg = self.format(record)
                
                # Mapper les niveaux de logging
                level_mapping = {
                    logging.DEBUG: 'DEBUG',
                    logging.INFO: 'INFO',
                    logging.WARNING: 'WARNING',
                    logging.ERROR: 'ERROR',
                    logging.CRITICAL: 'ERROR'
                }
                level = level_mapping.get(record.levelno, 'INFO')
                
                # Ajouter le log à la tâche
                task.add_log(msg, level)
        except Exception:
            # En cas d'erreur, ne pas bloquer l'exécution
            pass


class TaskLogger:
    """
    Logger personnalisé pour les tâches avec suivi de progression.
    Simplifie l'utilisation du logging dans les fonctions de tâches.
    """
    
    def __init__(self, task_id: str, logger_name: str = 'anarisk.task'):
        self.task_id = task_id
        self.task = task_manager.get_task(task_id)
        
        # Créer un logger spécifique pour cette tâche
        self.logger = logging.getLogger(f"{logger_name}.{task_id}")
        self.logger.setLevel(logging.DEBUG)
        
        # Supprimer les handlers existants pour éviter les doublons
        self.logger.handlers.clear()
        self.logger.propagate = False
        
        # Ajouter notre handler personnalisé
        self.handler = TaskLogHandler(task_id)
        self.logger.addHandler(self.handler)
        
        # Ajouter aussi un handler console pour le debugging
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            f'[TASK {task_id[:8]}] %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(console_handler)
    
    def debug(self, message: str):
        """Log de niveau DEBUG"""
        self.logger.debug(message)
    
    def info(self, message: str):
        """Log de niveau INFO"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log de niveau WARNING"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log de niveau ERROR"""
        self.logger.error(message)
    
    def exception(self, message: str):
        """Log d'exception avec traceback"""
        self.logger.exception(message)
        # Ajouter aussi au TaskManager
        if self.task:
            self.task.add_log(message, 'ERROR')
    
    def success(self, message: str):
        """Log de succès (équivalent à INFO mais avec niveau SUCCESS)"""
        if self.task:
            self.task.add_log(message, 'SUCCESS')
        self.logger.info(message)
    
    def progress(self, current: int, total: int, step: str = ""):
        """
        Met à jour la progression de la tâche.
        
        Args:
            current: Valeur courante (ex: 50)
            total: Valeur totale (ex: 100)
            step: Description de l'étape en cours
        """
        if self.task and total > 0:
            percent = int((current / total) * 100)
            self.task.update_progress(percent, step)
            
            if step:
                self.logger.info(f"{step} ({current}/{total} - {percent}%)")
            else:
                self.logger.info(f"Progression: {current}/{total} ({percent}%)")
    
    def set_progress(self, percent: int, step: str = ""):
        """
        Définit directement le pourcentage de progression.
        
        Args:
            percent: Pourcentage de 0 à 100
            step: Description de l'étape en cours
        """
        if self.task:
            self.task.update_progress(percent, step)
            if step:
                self.logger.info(f"{step} ({percent}%)")
    
    def cleanup(self):
        """Nettoie les handlers pour éviter les fuites mémoire"""
        self.logger.removeHandler(self.handler)
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)


@contextmanager
def task_context(task_id: str, task_name: str, quantume: Optional[str] = None):
    """
    Gestionnaire de contexte pour l'exécution d'une tâche.
    Simplifie la gestion du cycle de vie d'une tâche.
    
    Usage:
        with task_context(task_id, "Analyse des risques", quantume="Q1_2026") as logger:
            logger.info("Démarrage de l'analyse...")
            logger.progress(50, 100, "Extraction des données")
            # ... code de la tâche ...
            return {'result': 'données'}
    
    Args:
        task_id: ID unique de la tâche
        task_name: Nom de la tâche
        quantume: Quantum associé (optionnel)
    
    Yields:
        TaskLogger: Logger configuré pour la tâche
    
    Raises:
        Exception: Toute exception levée dans le bloc est capturée et enregistrée
    """
    # Créer la tâche dans le TaskManager
    task = task_manager.create_task(task_id, task_name, quantume)
    
    # Créer le logger
    logger = TaskLogger(task_id)
    
    try:
        # Marquer la tâche comme démarrée
        task.start()
        logger.info(f"=== Début de la tâche '{task_name}' ===")
        
        # Exécuter le bloc de code
        yield logger
        
        # Si on arrive ici sans exception, la tâche est réussie
        if task.status.value == "RUNNING":
            task.complete()
            logger.success(f"=== Tâche '{task_name}' terminée avec succès ===")
    
    except Exception as e:
        # En cas d'erreur, marquer la tâche comme échouée
        error_msg = str(e)
        task.fail(error_msg)
        logger.error(f"=== Échec de la tâche '{task_name}' : {error_msg} ===")
        raise  # Re-lever l'exception pour que l'appelant puisse la gérer
    
    finally:
        # Nettoyer le logger
        logger.cleanup()


def create_task_logger(task_id: str) -> TaskLogger:
    """
    Crée un logger pour une tâche existante.
    À utiliser quand la tâche a déjà été créée manuellement.
    
    Args:
        task_id: ID de la tâche existante
    
    Returns:
        TaskLogger: Logger configuré
    """
    return TaskLogger(task_id)


def log_to_task(task_id: str, message: str, level: str = 'INFO'):
    """
    Fonction utilitaire pour ajouter un log directement à une tâche.
    
    Args:
        task_id: ID de la tâche
        message: Message à logger
        level: Niveau du log (INFO, WARNING, ERROR, SUCCESS)
    """
    task = task_manager.get_task(task_id)
    if task:
        task.add_log(message, level)


# Configuration du logging global de l'application
def setup_app_logging(level=logging.INFO):
    """
    Configure le logging global de l'application.
    À appeler au démarrage de l'application Flask.
    
    Args:
        level: Niveau de logging par défaut
    """
    # Logger principal de l'application
    app_logger = logging.getLogger('anarisk')
    app_logger.setLevel(level)
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Format des logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Ajouter le handler si pas déjà présent
    if not app_logger.handlers:
        app_logger.addHandler(console_handler)
    
    return app_logger
