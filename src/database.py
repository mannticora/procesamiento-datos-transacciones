import os
import mysql.connector
from mysql.connector import Error, errorcode
from dotenv import load_dotenv
from typing import Optional, Union, Dict, List

load_dotenv()


class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'prueba_tecnica_db'),
            'auth_plugin': 'mysql_native_password',
            'raise_on_warnings': True
        }
        self.connection = None

    def connect(self, attempts: int = 3) -> bool:
        for attempt in range(1, attempts + 1):
            try:
                self.connection = mysql.connector.connect(**self.config)
                if self.connection.is_connected():
                    print(f"✅ Conexión exitosa (Intento {attempt}/{attempts})")
                    return True
            except Error as err:
                print(f"Intento {attempt} fallido: {err}")
                if attempt < attempts:
                    self._wait_before_retry(attempt)
        return False

    def _wait_before_retry(self, attempt: int):
        import time
        wait_time = min(2 ** attempt, 10)  # Backoff exponencial con máximo 10 seg
        print(f"Esperando {wait_time}s antes de reintentar...")
        time.sleep(wait_time)

    def execute_query(self, query: str, params: Union[tuple, Dict] = None,
                      fetch: bool = False) -> Optional[List[Dict]]:
        cursor = None
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())

            if fetch:
                return cursor.fetchall()
            self.connection.commit()
            return None

        except Error as e:
            print(f"Error ejecutando query: {e}")
            self.connection.rollback()
            return None
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("🔌 Conexión cerrada")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# Ejemplo de uso
if __name__ == "__main__":
    with DatabaseManager() as db:
        result = db.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s LIMIT 5",
            (os.getenv('DB_NAME', 'prueba_tecnica_db'),),
            fetch=True
        )
        print("Tablas encontradas:", result)