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


POSTGRES_DB_HOST = os.getenv("ODS_PG_HOST", "10.3.1.129")
POSTGRES_DB_PORT = os.getenv("ODS_PG_PORT", "5432")
POSTGRES_DB_USER = os.getenv("ODS_PG_USER", "postgres")
POSTGRES_DB_PASSWORD = os.getenv("ODS_PG_PASSWORD", "cpf2022")
POSTGRES_DB_NAME = os.getenv("ODS_PG_DB", "postgres")


# Initialisation des connexions aux bases de données
oracle_engine = connectionOds()

print(
    f"Postgres DB Host: {POSTGRES_DB_HOST} Port: {POSTGRES_DB_PORT} User: {POSTGRES_DB_USER} DB: {POSTGRES_DB_NAME}"
)
