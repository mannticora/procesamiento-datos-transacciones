import mysql.connector
from mysql.connector import Error


def create_daily_transactions_view(host, user, password, database):
    """
    Crea la vista para ver montos totales por día y compañía

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
            password=password,
            database=database
        )

        cursor = connection.cursor()

        # Crear vista
        with open('create_view.sql', 'r') as sql_file:
            sql_script = sql_file.read()

        cursor.execute(sql_script)
        connection.commit()
        print("Vista daily_company_transactions creada exitosamente")

    except Error as e:
        print(f"Error al crear la vista: {e}")
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

    create_daily_transactions_view(**db_config)