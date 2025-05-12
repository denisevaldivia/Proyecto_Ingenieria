import os
from dotenv import load_dotenv
import snowflake.connector
import glob
import csv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Función para elegir el env
def load_custom_env(username):
    env_file = f".env.{username}"

    # Si el archivo existe, lo carga
    if os.path.exists(env_file):
        load_dotenv(env_file)
    
    # Si no, imprime un error
    else:
        raise FileNotFoundError(f"Environment file {env_file} not found.")

class Snowflake:
    def __init__(self):
        self.account = os.getenv('account')
        self.user = os.getenv('user')
        self.password = os.getenv('password')
        self.warehouse = os.getenv('warehouse')
        self.database = os.getenv('database')
        self.schema = os.getenv('schema')
        self.stage = os.getenv('stage')  # El stage en Snowflake

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
        """Sube archivos CSV desde una carpeta local al stage en Snowflake."""

        # Buscar todos los CSV en la carpeta indicada
        try:
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            if not csv_files:
                print(f"No se encontraron archivos CSV en {folder_path}")
                return
            
            # Por cada CSV encontrado
            for local_file_path in csv_files:

                # Comando PUT para subirlo al stage de Snowflake
                put_command = f"PUT file://{local_file_path} @{stage_path} OVERWRITE = TRUE"
                self.cursor.execute(put_command)
                print(f"Archivo {os.path.basename(local_file_path)} subido a stage {stage_path}")

        except Exception as e:
            print(f"Error subiendo archivos desde {folder_path}: {e}")

    def extract_files_from_stage(self, stage):
        """Extract CSV files from the stage and read them into memory (no local save)."""
        try:
            # # Listar los archivos en el stage
            list_command = f"LIST @{stage}"
            self.cursor.execute(list_command)
            files = self.cursor.fetchall()

            if not files:
                print("No files found in the stage.")
                return []

            combined_data = pd.DataFrame()

            # Por cada archivo encontrado
            for file in files:
                file_name = os.path.basename(file[0])
                print(f"File found in stage: {file_name}")

                # Consulta para leer el contenido del CSV
                # Hay nulos por lo que se usa el Try_cast
                query = f"""
                SELECT TRY_CAST($1 AS INT) AS Rank, 
                TRY_CAST($2 AS INT) AS Previous_Rank, 
                $3 AS Artist_Name, 
                TRY_CAST($4 AS INT) AS Periods_on_Chart, 
                TRY_CAST($5 AS INT) AS Views, 
                TRY_CAST(REPLACE($6, '%', '') AS FLOAT) AS Growth,
                $7 AS Week, 
                $8 AS Month
                FROM @{stage}/{file_name} (FILE_FORMAT => 'PROJECT.CSV_FORMAT')
                """

                #  # Ejecuta la consulta para leer los datos
                self.cursor.execute(query)
                data = pd.DataFrame(self.cursor.fetchall(), columns=["Rank", "Previous_Rank", "Artist_Name", "Periods_on_Chart", "Views", "Growth", "Week", "Month"])

                print(f"{len(data)} records read from {file_name}")

                # Verificar si los datos contienen records
                if data.empty:
                    print(f"No data found in {file_name}. Skipping file.")
                    continue

                # Combina los datos leídos con el DataFrame acumulado
                combined_data = pd.concat([combined_data, data], ignore_index=True)

            # Verificar si los datos se pudieron combinar
            if combined_data.empty:
                print("No valid data extracted from the files.")
                return []

            # Mensaje indicador del proceso
            print(f"Files combined in memory with {len(combined_data)} records.")
            return combined_data

        except Exception as e:
            print(f"Error extracting and combining files: {e}")
            return None
    
    def create_table_if_not_exists(self, table_name, dataframe):
        """Crea la tabla en Snowflake si no existe, según las columnas del DataFrame"""
        try:
            # Construir query dinámica de creación
            columns = []
            for col in dataframe.columns:
                # Detecta el tipo de dato de cada columna
                dtype = dataframe[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    col_type = "NUMBER"
                elif pd.api.types.is_float_dtype(dtype):
                    col_type = "FLOAT"
                else:
                    col_type = "VARCHAR"

                columns.append(f'"{col}" {col_type}')

            # Arma el query de creación de la tabla RECORDS
            columns_sql = ", ".join(columns)

            create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            )
            """
            self.cursor.execute(create_query)
            print(f"Tabla {table_name} verificada/creada correctamente.")

        except Exception as e:
            print(f"Error creando tabla {table_name}: {e}")

    def load_data_to_snowflake(self, combined_data):
        """Carga los datos combinados directamente en la tabla RECORDS en Snowflake."""
        try:
            # Crear tabla si no existe
            self.create_table_if_not_exists('RECORDS', combined_data)

            # Ahora cargar los datos
            success, nchunks, nrows, _ = write_pandas(self.conn, combined_data, 'RECORDS')
            print(f"Se cargaron {nrows} registros en la tabla RECORDS")

        except Exception as e:
            print(f"Error cargando datos a Snowflake: {e}")

    def close_connection(self):
        """Cerrar conexión"""
        self.cursor.close()
        self.conn.close()
        print('Conexión a Snowflake cerrada')