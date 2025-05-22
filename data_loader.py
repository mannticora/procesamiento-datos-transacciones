import pandas as pd
import mysql.connector
from mysql.connector import Error
import os


def load_data_to_mysql(csv_path, host, user, password, database):
    """
    Carga datos desde un archivo CSV a una base de datos MySQL

    Args:
        csv_path (str): C:\Users\ricar\OneDrive\Escritorio\data_prueba_tecnica.csv
        host (str): localhost
        user (str): root
        password (str): MyNewPass1
        database (str): data_prueba_tecnica
    """

    try:
        # Leer el archivo CSV
        df = pd.read_csv(csv_path)

        # Conectar a MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

        cursor = connection.cursor()

        # Crear la base de datos si no existe
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cursor.execute(f"USE {database}")

        # Crear tabla temporal para cargar todos los datos
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id VARCHAR(255),
            company_id VARCHAR(255),
            company_name VARCHAR(255),
            amount VARCHAR(255),
            status VARCHAR(255),
            created_at VARCHAR(255),
            updated_at VARCHAR(255)
        """)

        # Limpiar tabla si ya existe
        cursor.execute("TRUNCATE TABLE raw_data")

        # Insertar datos
        for _, row in df.iterrows():
            sql = """
            INSERT INTO raw_data 
            (id, company_id, company_name, amount, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, tuple(row))

        connection.commit()
        print("Datos cargados exitosamente en la tabla raw_data")

    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    # Configuración
    csv_path = "C:/Users/ricar/OneDrive/Escritorio/data_prueba_tecnica.csv"
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "MyNewPass1",
        "database": "prueba_tecnica_db"
    }

    load_data_to_mysql(csv_path, **db_config)