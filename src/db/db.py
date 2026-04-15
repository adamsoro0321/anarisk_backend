from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy.orm import sessionmaker
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///:memory:")
engine = create_engine(DATABASE_URL,
                        pool_pre_ping=True,      # vérifie la connexion avant utilisation
                        pool_size=5,
                        max_overflow=10,
                        pool_recycle=280,        # recycle les connexions avant que Supabase les coupe
                        pool_timeout=20,
                         connect_args={"sslmode": "require"},
                        echo=True)
appSession= sessionmaker(bind=engine)

def get_db():
    db = appSession()
    try:
        yield db
    finally:
        db.close()