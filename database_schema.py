import mysql.connector
import pandas as pd
from mysql.connector import Error


def create_database_schema(host, user, password, database):
    """
    Crea el esquema de base de datos con las tablas charges y companies

    Args:
        host (str): localhost
        user (str): root
        password (str): MyNewPass1
        database (str): data_prueba_tecnica.csv
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

        cursor = connection.cursor()

        # Crear base de datos si no existe
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cursor.execute(f"USE {database}")

        # Crear tabla companies
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            company_id VARCHAR(24) PRIMARY KEY,
            company_name VARCHAR(130)
        )
        """)

        # Crear tabla charges
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS charges (
            id VARCHAR(24) PRIMARY KEY,
            company_id VARCHAR(24) NOT NULL,
            amount DECIMAL(16,2) NOT NULL,
            status VARCHAR(30) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NULL,
            FOREIGN KEY (company_id) REFERENCES companies(company_id)
        )
        """)

        connection.commit()
        print("Esquema de base de datos creado exitosamente")

    except Error as e:
        print(f"Error al crear el esquema: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def load_transformed_data(host, user, password, database, data_file):
    """

    Args:
        host (str): localhost
        user (str): root
        password (str): MyNewPass1
        database (str): data_prueba_tecnica.csv
        data_file (str): data_prueba_tecnica2.csv
    """
    try:

        df = pd.read_parquet(data_file)

        # Conectar a MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        cursor = connection.cursor()

        # 1. Cargar datos en companies
        companies_data = df[['company_id', 'company_name']].drop_duplicates()

        for _, row in companies_data.iterrows():
            cursor.execute("""
            INSERT IGNORE INTO companies (company_id, company_name)
            VALUES (%s, %s)
            """, (row['company_id'], row['company_name']))

        # 2. Cargar datos en charges
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO charges 
            (id, company_id, amount, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['id'], row['company_id'], row['amount'],
                row['status'], row['created_at'], row['updated_at']
            ))

        connection.commit()
        print("Datos cargados exitosamente en las tablas charges y companies")

    except Error as e:
        print(f"Error al cargar datos: {e}")
        connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    # Configuración
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "MyNewPass1",
        "database": "prueba_tecnica_db"
    }

    create_database_schema(**db_config)

    load_transformed_data(**db_config, data_file='transformed_data.parquet')