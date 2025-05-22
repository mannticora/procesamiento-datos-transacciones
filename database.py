import os
import mysql.connector
from mysql.connector import Error, errorcode
from dotenv import load_dotenv
from typing import Optional, Union, Dict


def create_db_connection(
        host: str = None,
        user: str = None,
        password: str = None,
        database: str = None,
        attempts: int = 3
) -> Optional[mysql.connector.connection.MySQLConnection]:
    """

    Args:
        host (str): localhost
        user (str): root
        password (str): MyNewPass1
        database (str): data_prueba_tecnica.csv
        attempts (int): Intentos de reconexión

    Returns:
        MySQLConnection object or None
    """

    load_dotenv()

    # Configuración por defecto desde variables de entorno
    config = {
        'host': host or os.getenv('DB_HOST', 'localhost'),
        'user': user or os.getenv('DB_USER', 'root'),
        'password': password or os.getenv('DB_PASSWORD', ''),
        'database': database or os.getenv('DB_NAME', 'prueba_tecnica_db'),
        'auth_plugin': 'mysql_native_password',
        'connect_timeout': 10
    }

    connection = None
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                print(f"Conexión exitosa a MySQL (Intento {attempt}/{attempts})")
                return connection

        except Error as err:
            last_error = err
            error_msg = f"❌ Error en intento {attempt}/{attempts}: "

            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                error_msg += "Credenciales incorrectas"
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                error_msg += f"La base de datos {config['database']} no existe"
            else:
                error_msg += str(err)

            print(error_msg)

            # Esperar antes de reintentar
            if attempt < attempts:
                import time
                time.sleep(2 ** attempt)  # Backoff exponencial

    print(f"No se pudo conectar después de {attempts} intentos. Último error: {last_error}")
    return None


def execute_query(
        connection: mysql.connector.connection.MySQLConnection,
        query: str,
        params: Union[tuple, Dict] = None,
        fetch: bool = False
) -> Optional[list]:
    """
    Ejecuta una consulta SQL con manejo seguro de parámetros.

    Args:
        connection: Conexión activa
        query: Consulta SQL
        params: Parámetros para la consulta
        fetch: Si debe retornar resultados

    Returns:
        Lista de resultados o None
    """
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)

        if fetch:
            return cursor.fetchall()
        else:
            connection.commit()
            return None

    except Error as e:
        print(f"Error al ejecutar consulta: {e}")
        connection.rollback()
        return None
    finally:
        if cursor:
            cursor.close()


def close_db_connection(connection: mysql.connector.connection.MySQLConnection) -> None:
    """Cierra la conexión de manera segura"""
    if connection and connection.is_connected():
        connection.close()
        print("🔌 Conexión cerrada correctamente")


# Ejemplo de uso
if __name__ == "__main__":
    conn = create_db_connection()

    if conn:

        result = execute_query(
            conn,
            "SELECT * FROM information_schema.tables WHERE table_schema = %s LIMIT 5",
            (os.getenv('DB_NAME', 'prueba_tecnica_db'),),
            fetch=True
        )

        if result:
            print("Tablas en la base de datos:")
            for row in result:
                print(f"- {row['TABLE_NAME']}")

        close_db_connection(conn)