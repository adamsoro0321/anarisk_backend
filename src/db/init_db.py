import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add src to path to import models and db connection
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from models import Base, User
from db.pg_connection import connect_pg
from globals import POSTGRES_DB_HOST, POSTGRES_DB_PORT, POSTGRES_DB_USER, POSTGRES_DB_PASSWORD, POSTGRES_DB_NAME

def init_db():
    host = POSTGRES_DB_HOST
    port = POSTGRES_DB_PORT
    user = POSTGRES_DB_USER
    password = POSTGRES_DB_PASSWORD
    database = POSTGRES_DB_NAME

    if not all([host, port, user, password, database]):
        print("Error: Missing database configuration in globals/env")
        return

    engine = connect_pg(user, password, host, port, database)
    
    if engine:
        print("Creating tables...")
        Base.metadata.create_all(engine)
        print("Tables created.")
        
        # Create a default admin user if not exists
        Session = sessionmaker(bind=engine)
        session = Session()
        
        admin_mail = "admin@dgi.sn"
        existing_user = session.query(User).filter_by(mail=admin_mail).first()
        
        if not existing_user:
            print(f"Creating default admin user: {admin_mail}")
            admin = User(
                nom="Admin",
                prenom="System",
                mail=admin_mail,
                role="admin",
                status="active"
            )
            admin.set_password("admin123") # Change this in production!
            session.add(admin)
            session.commit()
            print("Default admin user created.")
        else:
            print("Admin user already exists.")
            
        session.close()
    else:
        print("Failed to connect to database.")

if __name__ == "__main__":
    init_db()
