from dotenv import load_dotenv
import os

def load_custom_env(username):
    env_file = f".env.{username}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")
    
load_custom_env('rocha')

class MySQLoaders:
    def __init__(self):
        self.host = os.getenv('HOST')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.database = os.getenv('DATABASE')

        # Conexión a MySQL
        self.conn = pymysql.connect(host=self.host,user=self.user,password=self.password,database=self.database)
        self.cursor = self.conn.cursor()