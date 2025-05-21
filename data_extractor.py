import pandas as pd
import mysql.connector
from mysql.connector import Error


def extract_data_from_mysql(host, user, password, database, output_format='parquet'):
    """
    Extrae datos de MySQL y los guarda en el formato especificado

    Args:
        host (str): Host de la base de datos
        user (str): Usuario de la base de datos
        password (str): Contraseña del usuario
        database (str): Nombre de la base de datos
        output_format (str): Formato de salida (csv, parquet)
    """
    try:
        # Conectar a MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Leer datos directamente a pandas
        query = "SELECT * FROM raw_data"
        df = pd.read_sql(query, connection)

        # Guardar en el formato especificado
        if output_format == 'parquet':
            df.to_parquet('extracted_data.parquet', index=False)
            print("Datos extraídos y guardados en extracted_data.parquet")
        elif output_format == 'csv':
            df.to_csv('extracted_data.csv', index=False)
            print("Datos extraídos y guardados en extracted_data.csv")
        else:
            raise ValueError("Formato no soportado. Usar 'csv' o 'parquet'")

    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
    except Exception as e:
        print(f"Error durante la extracción: {e}")
    finally:
        if connection.is_connected():
            connection.close()


if __name__ == "__main__":
    # Configuración
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "tu_contraseña",  # Cambiar por tu contraseña
        "database": "prueba_tecnica_db"
    }

    extract_data_from_mysql(**db_config, output_format='parquet')