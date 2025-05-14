# Proyecto_Ingenieria

Este proyecto es un ETL que analiza los datos semanales de YouTube Charts a través de Snowflake y PowerBI. Se utiliza Snowflake para almacenar y transformar los datos, mientras que se analizan en PowerBI.

## Requerimientos

Las librerías necesarias para correr el código se encuentran en el archivo de texto **_requirements.txt_**.

## Uso de Credenciales

Es necesario tener una cuenta en Snowflake para correr el ETL. Las credenciales de inicio de sesión deben de almacenarse en un archivo **_.env_**. Este código permite que múltiples usuarios tengan acceso al ETL, por lo que se pueden almacenar varias credenciales a la vez. Entonces, si el usuario que desea acceder al ETL se llama "moy", su archivo debe de nombrarse de la siguiente manera: **_.env.moy_**.

Este archivo env debe de contener los siguientes datos:

> user='YOUR_USERNAME'
>
> password='YOUR_PASSWORD'
>
> account='YOUR_ACCOUNT_URL'
>
> warehouse='COMPUTE_WH'
>
> database='PID_DATAWAREHOUSE'
>
> schema='PROJECT'

## Set-Up en Snowflake

Para que el cargado y transformación de datos a Snowflake sea correcto, se necesita:

1. Crear una stage llamada **_PROJECT_DATALAKE_**, 
2. Crear una Base de Datos en Snowflake con el nombre **_PID_DATAWAREHOUSE_**, y 
3. Crear un schema dentro de la Base de Datos llamado **_PROJECT_**.

**NOTA: El usuario puede cambiar los nombres de cada stage, Base de Datos o Schema. Sin embargo, para simplificar el proceso y reducir el número de cambios al código base, sugerimos los mencionados.

Además, se necesita correr el siguiente código de SQL en Snowflake:

> CREATE FILE FORMAT PROJECT.CSV_FORMAT 
>
>TYPE = 'CSV' 
>
>FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
>
>COMPRESSION = 'GZIP' 
>
>FIELD_DELIMITER = ',' 
>
>SKIP_HEADER = 1;

## Ejecución

Para inicializar el ETL, es necesario ingresar los datos deseados a la carpeta "datos_csv". Ahora mismo, la carpeta cuenta con  los datos semanales de todo el año 2024 de YouTube Charts. En caso de querer analizar otros años, es necesario descargar los datos semanales y guardarlos en la carpeta "datos_csv". 

Posteriormente, es necesario correr el archivo **_orchestrations.py_**. En este, se debe especificar el nombre del usuario que va a acceder a Snowflake a través de la función **_load_custom_env_**. El nombre de usuario es el especificado en el archivo **_.env_**. Siguiendo el ejemplo anterior, el usuario debería de insertar: **_load_custom_env('moy')_**.

Esto activará el ETL y comenzará a mostrar mensajes de su avance. En caso de que algo impida la ejecución correcta, se le mostrará un mensaje de error indicándole la parte del ETL que falló.

Para visualizar el dashboard en PowerBI, es necesario descargar el archvivo **_YouTube Charts.pbix _** y realizar la conexión con Snowflake para cargar los datos, ingresando las credenciales que se encuentran en el archivo **_.env_**.
