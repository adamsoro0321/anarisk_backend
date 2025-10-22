from dotenv import load_dotenv

load_dotenv()

import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def connectionESIntaxDB():
    """
    Connect to a MySQL database using SQLAlchemy and mysqlconnector.
    Returns:
        sqlalchemy.Engine: The SQLAlchemy engine object, or None if an error occurs.
    """
    user = os.getenv("es_USER")
    password = os.getenv("es_PASSWORD")
    host = os.getenv("es_HOST")
    port = os.getenv("es_PORT")
    database = os.getenv("es_DATABASE")
    # Check for missing environment variables
    if not all([user, password, host, port, database]):
        print("Error: Missing one or more required environment variables.")
        return None

    # Encode password to avoid issues with special characters
    password = quote_plus(password)
    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    # url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    try:
        engine = create_engine(url)
        return engine
    except Exception as e:
        print(f"Error: {e}")
        return None
