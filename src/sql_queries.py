from database import DatabaseManager
from dotenv import load_dotenv
import os

load_dotenv()


def create_daily_transactions_view() -> bool:
    try:
        with DatabaseManager() as db:
            if not db.connection:
                return False

            # Ejecutar script SQL de vista
            with open('./sql/views.sql', 'r') as f:
                view_script = f.read()
                db.execute_query(view_script)

            # Verificar creación
            result = db.execute_query("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = %s 
                AND table_name = 'daily_company_transactions'
            """, (os.getenv('DB_NAME', 'prueba_tecnica_db'),), fetch=True)

            if result:
                print("Vista 'daily_company_transactions' creada")
                return True
            return False

    except Exception as e:
        print(f"Error creando vista: {e}")
        return False


def get_daily_transactions(date_from=None, date_to=None):
    try:
        with DatabaseManager() as db:
            if not db.connection:
                return None

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

            return db.execute_query(query, tuple(params) if params else db.execute_query(query), fetch=True)

    except Exception as e:
        print(f"Error consultando vista: {e}")
        return None


if __name__ == "__main__":
    success = create_daily_transactions_view()
    print("Creación de vista:", "Éxito" if success else "Falló")

    print("\nEjemplo de datos:")
    transactions = get_daily_transactions()
    if transactions:
        for tx in transactions[:3]:  # Mostrar primeras 3 transacciones
            print(f"{tx['transaction_date']} | {tx['company_name']}: ${tx['total_amount']}")