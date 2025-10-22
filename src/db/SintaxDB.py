from dotenv import load_dotenv
load_dotenv() 
import os
from sqlalchemy import create_engine


user = os.getenv("s_USER")
password = os.getenv("s_PASSWORD")
host = os.getenv("s_HOST")
port = os.getenv("s_PORT")
database = os.getenv("s_DATABASE")

#connect sqlalchemy to oracle database using oracle+oracledb
#oracle+oracledb


def connectionSIntaxDB():
    """
    
    return: engine
    """
    #gere les exceptions
    url =f'oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={database}'
    try:
        engine = create_engine(url)
        print(f"Connection to oracle {database} database successful")
        return engine
    except Exception as e:
        print(f"Error: {e}")
        return None
   



