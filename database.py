# database.py
import sqlite3
import pandas as pd

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def execute_query(self, query, params=None):
        with self.get_connection() as conn:
            if params:
                if isinstance(params, dict):
                    params = tuple(params.values())
                elif not isinstance(params, (tuple, list)):
                    params = (params,)
                
                if isinstance(params, list):
                    params = tuple(params)
                    
                return pd.read_sql_query(query, conn, params=params)
            else:
                return pd.read_sql_query(query, conn)