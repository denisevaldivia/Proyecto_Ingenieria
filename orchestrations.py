from data_storage import load_custom_env, Snowflake

# Cargar env
load_custom_env('diana')

# Instanciar la clase
etl = Snowflake()

# Subir archivos locales al stage
local_folder_path = 'datos_csv'
stage_path = 'PROJECT_DATALAKE'
etl.put_csv_folder_to_stage(local_folder_path, stage_path)

# Extraer y combinar archivos desde el stage (en memoria)
combined_data = etl.extract_files_from_stage(stage_path)

# Cargar los datos combinados en la tabla RECORDS (y crearla)
etl.load_data_to_snowflake(combined_data)

# Cerrar conexión al final
etl.close_connection()
