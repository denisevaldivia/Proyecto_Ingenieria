from dotenv import load_dotenv
import os
import snowflake.connector

def load_custom_env(username):
    env_file = f".env.{username}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")
    
load_custom_env('rocha')
class SnowflakeLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            account=os.getenv('HOST'),
            warehouse=os.getenv('WAREHOUSE'),
            database=os.getenv('DATABASE'),
            schema=os.getenv('SCHEMA')
        )
        self.cursor = self.conn.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()