
import pandas as pd
from database import DatabaseManager
from dotenv import load_dotenv
import os

load_dotenv()

def load_raw_data():
    try:

        df = pd.read_csv(os.getenv('DATA_RAW_PATH'))
        print(f"Datos cargados desde CSV ({len(df)} registros)")

        with DatabaseManager() as db:
            if not db.connect():
                return False

            # Crear tabla temporal para datos crudos
            db.execute_query("""
                CREATE TABLE IF NOT EXISTS raw_data (
                    id VARCHAR(255),
                    company_id VARCHAR(255),
                    company_name VARCHAR(255),
                    amount VARCHAR(255),
                    status VARCHAR(255),
                    created_at VARCHAR(255),
                    updated_at VARCHAR(255)
                )
            """)

            for _, row in df.iterrows():
                db.execute_query("""
                    INSERT INTO raw_data 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))

            print("Datos cargados en tabla 'raw_data'")
            return True

    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    load_raw_data()