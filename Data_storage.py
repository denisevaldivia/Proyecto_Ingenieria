import os
from dotenv import load_dotenv
import snowflake.connector
import glob
import csv

# Función para elegir el env
def load_custom_env(username):
    env_file = f".env.{username}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")

# Elegir el env
load_custom_env('diana')


class SnowflakeLoaders:
    def __init__(self):
        self.account = os.getenv('account')
        self.user = os.getenv('user')
        self.password = os.getenv('password')
        self.warehouse = os.getenv('warehouse')
        self.database = os.getenv('database')
        self.schema = os.getenv('schema')

        # Conexión a Snowflake
        self.conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        self.cursor = self.conn.cursor()

    def put_csv_folder_to_stage(self, folder_path, stage_path):
        try:
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            if not csv_files:
                print(f"No se encontraron archivos CSV en {folder_path}")
                return

            for local_file_path in csv_files:
                put_command = f"PUT file://{local_file_path} @{stage_path} OVERWRITE = TRUE"
                self.cursor.execute(put_command)
                print(f"Archivo {os.path.basename(local_file_path)} subido a stage {stage_path}")

        except Exception as e:
            print(f"Error subiendo archivos desde {folder_path}: {e}")

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
        print('Conexión a Snowflake cerrada')


# Ejecutar ETL
etl = SnowflakeLoaders()

# Carpeta local con los CSV
local_folder_path = 'datos_csv'

# Stage destino
stage_path = 'PROJECT_DATALAKE'

# Subir todos los CSV de la carpeta al stage
etl.put_csv_folder_to_stage(local_folder_path, stage_path)

# Cerrar conexión después
etl.close_connection()
