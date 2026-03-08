import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Flask
    SECRET_KEY = os.getenv('SECRET_KEY', 'anarisk-dgi-secret')
    JWT_EXPIRATION_HOURS = int(os.getenv('JWT_EXPIRATION_HOURS', 24))
    
    # Database Oracle (pour les données SINTAX)
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '1521')
    DB_SERVICE = os.getenv('DB_SERVICE')
    
    # Database PostgreSQL (pour l'application - users, roles, etc.)
    POSTGRES_DB_HOST = os.getenv('POSTGRES_DB_HOST', 'localhost')
    POSTGRES_DB_PORT = os.getenv('POSTGRES_DB_PORT', '5432')
    POSTGRES_DB_USER = os.getenv('POSTGRES_DB_USER', 'anarisk')
    POSTGRES_DB_PASSWORD = os.getenv('POSTGRES_DB_PASSWORD', 'anarisk')
    POSTGRES_DB_NAME = os.getenv('POSTGRES_DB_NAME', 'anarisk_db')
    
    # SQLAlchemy Configuration
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{POSTGRES_DB_USER}:{POSTGRES_DB_PASSWORD}@"
        f"{POSTGRES_DB_HOST}:{POSTGRES_DB_PORT}/{POSTGRES_DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = os.getenv('SQLALCHEMY_ECHO', 'False').lower() == 'true'
    
    # Redis/Celery
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    
    # Paths
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / 'data'
    LOGS_DIR = BASE_DIR / 'logs'
    
    # Application
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB
    CHUNK_SIZE = 1000  # Pour le traitement par lots
    
    @classmethod
    def init_app(cls):
        cls.LOGS_DIR.mkdir(exist_ok=True)
        cls.DATA_DIR.mkdir(exist_ok=True)