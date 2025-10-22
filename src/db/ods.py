from dotenv import load_dotenv

import os
from sqlalchemy import create_engine

load_dotenv()
user = os.getenv("ods_user")
password = os.getenv("ods_user_password")
host = os.getenv("ods_host")
port = os.getenv("ods_port")
database = os.getenv("ods_db")


def connectionOds():
    """
    return: engine
    """
    # gere les exceptions
    url = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={database}"
    try:
        engine = create_engine(url)
        print(f"Connection to oracle {database} database successful")
        return engine
    except Exception as e:
        print(f"Error: {e}")
        return None
    # def connectionOdsDB():
