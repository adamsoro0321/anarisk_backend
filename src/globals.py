import os
from db.ods import connectionOds

from db.pg_connection import connect_pg
from sqlalchemy.orm import sessionmaker

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


POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST", "127.0.0.1")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT", "5432")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER", "postgres")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD", "123456")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "anarisk")


# Initialisation des connexions aux bases de données
oracle_engine = connectionOds()

print(
    f"Postgres DB Host: {POSTGRES_DB_HOST} Port: {POSTGRES_DB_PORT} User: {POSTGRES_DB_USER} DB: {POSTGRES_DB_NAME}"
)

pg_engine = connect_pg(
    POSTGRES_DB_USER,
    POSTGRES_DB_PASSWORD,
    POSTGRES_DB_HOST,
    POSTGRES_DB_PORT,
    POSTGRES_DB_NAME
)

Session = sessionmaker(bind=pg_engine) if pg_engine else None
