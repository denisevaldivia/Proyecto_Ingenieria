import os
from dotenv import load_dotenv
import snowflake.connector

# Function to load custom env file
def load_custom_env(username):
    env_file = f".env.{username}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")

# Load your personal env file here (can be dynamically set later)
load_custom_env('rocha')

# Snowflake connector class
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

# Example usage
if __name__ == "__main__":
    db = SnowflakeLoader()

    result = db.execute_query("SELECT CURRENT_DATE;")
    print(result)

    db.close_connection()
