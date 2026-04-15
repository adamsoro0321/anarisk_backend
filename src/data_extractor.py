import pandas as pd
class DataExtractor:
    def __init__(self):
        pass

    def extract_data(self, con, sql):
        try:
            data = pd.read_sql_query(sql, con)
            return data
        except Exception as e:
            print(f"An error occurred: {e}")
            return None