from dotenv import load_dotenv
load_dotenv() 
import os
from sqlalchemy import create_engine

def connect_pg(user,password,host,port,database):
    """
      return: engine
    """
    #gere les exceptions
    url =f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    try:
        engine = create_engine(url)
        print(f"engine {database} created")
        return engine
    except Exception as e:
        print(f"Error: {e}")
        return None
