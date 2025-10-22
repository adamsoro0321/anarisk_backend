from dotenv import load_dotenv
load_dotenv() 
import os
from sqlalchemy import create_engine

def connect_oracle(user,password,host,port,database):
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
