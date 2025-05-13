# Proyecto_Ingenieria

Este proyecto es un ETL que analiza los datos semanales de YouTube Charts a través de Snowflake y PowerBI. Se utiliza Snowflake para almacenar y transformar los datos, mientras que se analizan en PowerBI .

## Requerimientos

Las librerías necesarias para correr el código se encuentran en el archivo de texto _requirements.txt_.

## Uso de Credenciales

Es necesario tener una cuenta en Snowflake para correr el ETL. Las credenciales de inicio de sesión deben de almacenarse en un archhivo _.env_

Para inicializar el ETL, es necesario descargar los datos semanales a la carpeta "datos_csv". Ahora mismo, están los datos semanales de todo el año 2024.

Posteriormente, es necesario correr el archivo _orchestrations.py_, que 