from mysql.connector import Error

from database_utils import create_db_connection, execute_sql_file


def create_daily_transactions_view():
    """Crea la vista de transacciones diarias"""
    connection = create_db_connection()
    if connection:
        try:
            # Ejecutar el script SQL
            execute_sql_file(connection, 'sql/create_view.sql')

            # Verificar que la vista se creó
            cursor = connection.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = DATABASE()
                AND table_name = 'daily_company_transactions'
            """)

            if cursor.fetchone():
                print("Vista 'daily_company_transactions' creada exitosamente")
            else:
                print("Error: La vista no se creó correctamente")

        except Error as e:
            print(f"Error al crear la vista: {e}")
        finally:
            if connection.is_connected():
                connection.close()


if __name__ == "__main__":
    create_daily_transactions_view()