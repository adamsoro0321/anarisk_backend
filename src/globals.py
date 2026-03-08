import os
from db.ods import connectionOds

# Configuration des variables d'environnement

# Variables globales pour partager entre les modules
risk_analyzer = None


def set_risk_analyzer(analyzer):
    """Définit l'analyseur de risques global"""
    global risk_analyzer
    risk_analyzer = analyzer


def get_risk_analyzer():
    """Récupère l'analyseur de risques global"""
    return risk_analyzer


# Configuration PostgreSQL (utilisée uniquement pour les variables d'environnement)
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST", "127.0.0.1")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER", "postgres")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD", "123456")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "anarisk")

print(f"🔧 Configuration de la base de données: {POSTGRES_DB_USER}@{POSTGRES_DB_HOST}:{POSTGRES_DB_PORT}/{POSTGRES_DB_NAME}")

# Initialisation de la connexion Oracle (pour les données sources)
oracle_engine = connectionOds()
