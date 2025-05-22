from database import DatabaseManager
from dotenv import load_dotenv
import os

load_dotenv()


def create_schema() -> bool:
    try:
        with DatabaseManager() as db:
            if not db.connection:
                return False

            # Ejecutar script SQL completo
            with open('./sql/schema.sql', 'r') as f:
                sql_script = f.read()
                for statement in sql_script.split(';'):
                    if statement.strip():
                        db.execute_query(statement)

            print("Esquema de base de datos creado")
            return True

    except Exception as e:
        print(f"error creando esquema: {e}")
        return False


def load_transformed_data(parquet_path: str) -> bool:
    try:
        import pandas as pd

        # Leer datos transformados
        df = pd.read_parquet(parquet_path)

        with DatabaseManager() as db:
            if not db.connection:
                return False

            # Insertar compañías
            companies = df[['company_id', 'company_name']].drop_duplicates()
            for _, row in companies.iterrows():
                db.execute_query("""
                    INSERT IGNORE INTO companies (company_id, company_name)
                    VALUES (%s, %s)
                """, (row['company_id'], row['company_name']))

            # Insertar cargos
            for _, row in df.iterrows():
                db.execute_query("""
                    INSERT INTO charges 
                    (id, company_id, amount, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['id'], row['company_id'], float(row['amount']),
                    row['status'], row['created_at'], row['updated_at']
                ))

            print(f"Datos cargados en tablas normalizadas")
            return True

    except Exception as e:
        print(f"Error cargando datos transformados: {e}")
        return False


if __name__ == "__main__":
    # Crear esquema primero
    schema_created = create_schema()

    if schema_created:
        # Cargar datos transformados
        parquet_path = os.path.join(
            os.getenv('DATA_PROCESSED_DIR', './data/processed'),
            'transformed_data.parquet'
        )
        load_success = load_transformed_data(parquet_path)
        print("Carga de datos:", "Éxito" if load_success else "Falló")