import os
from src.data_loader import load_raw_data
from src.data_extractor import extract_to_parquet
from src.data_transformer import transform_data
from src.schema_manager import create_schema, load_transformed_data
from src.sql_queries import create_daily_transactions_view
from dotenv import load_dotenv

load_dotenv()


def run_etl_pipeline():
    print("🚀 Iniciando proceso ETL completo\n")

    # 1. Carga de datos inicial
    print("=== ETAPA 1: Cargando datos iniciales ===")
    raw_data_path = os.getenv('DATA_RAW_PATH', './data/raw/data_prueba_tecnica.csv')
    if not load_raw_data(raw_data_path):
        print("Falló la carga inicial de datos")
        return False

    # 2. Extracción a Parquet
    print("\n=== ETAPA 2: Extrayendo datos a Parquet ===")
    extracted_path = os.path.join(os.getenv('DATA_PROCESSED_DIR', './data/processed'), 'extracted_data.parquet')
    if not extract_to_parquet(extracted_path):
        print("Falló la extracción de datos")
        return False

    # 3. Transformación
    print("\n=== ETAPA 3: Transformando datos ===")
    transformed_path = os.path.join(os.getenv('DATA_PROCESSED_DIR', './data/processed'), 'transformed_data.parquet')
    if not transform_data(extracted_path, transformed_path):
        print("Falló la transformación de datos")
        return False

    # 4. Creación de esquema y carga en tablas normalizadas
    print("\n=== ETAPA 4: Creando esquema y cargando datos transformados ===")
    if not create_schema():
        print("Falló la creación del esquema")
        return False

    if not load_transformed_data(transformed_path):
        print("Falló la carga de datos transformados")
        return False

    # 5. Creación de vista SQL
    print("\n=== ETAPA 5: Creando vista de transacciones diarias ===")
    if not create_daily_transactions_view():
        print("Fallo la creación de la vista")
        return False

    print("\n Proceso ETL completado exitosamente!")
    return True


if __name__ == "__main__":
    run_etl_pipeline()