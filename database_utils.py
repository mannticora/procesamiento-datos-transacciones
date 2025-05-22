import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os


def create_db_connection():
    """Crea conexión a la base de datos MySQL"""
    load_dotenv()
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'prueba_tecnica_db')
        )
        return connection
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None


def execute_sql_file(connection, file_path):

    try:
        cursor = connection.cursor()

        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()

        # Ejecutar cada sentencia SQL separada por ;
        for statement in sql_script.split(';'):
            if statement.strip():
                cursor.execute(statement)

        connection.commit()
        print("Script SQL ejecutado correctamente")
    except Error as e:
        print(f"Error al ejecutar SQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()

    def get_daily_transactions(connection, date_from=None, date_to=None):
        """Obtiene datos de la vista"""
        try:
            cursor = connection.cursor(dictionary=True)

            query = """
            SELECT * FROM daily_company_transactions 
            WHERE 1=1
            """
            params = []

            if date_from:
                query += " AND transaction_date >= %s"
                params.append(date_from)
            if date_to:
                query += " AND transaction_date <= %s"
                params.append(date_to)

            query += " ORDER BY transaction_date DESC, total_amount DESC"

            cursor.execute(query, params)
            return cursor.fetchall()
        except Error as e:
            print(f"Error al consultar vista: {e}")
            return None