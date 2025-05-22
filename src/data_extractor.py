import pandas as pd
from database import DatabaseManager
from dotenv import load_dotenv
import os

load_dotenv()


def extract_to_parquet(output_path: str) -> bool:
    try:
        with DatabaseManager() as db:
            if not db.connection:
                return False

            # Leer datos directamente a DataFrame
            df = pd.read_sql("SELECT * FROM raw_data", db.connection)

            # Guardar como Parquet
            df.to_parquet(output_path, index=False)
            print(f"Datos extraídos a {output_path}")
            return True

    except Exception as e:
        print(f"Error extrayendo datos: {e}")
        return False


if __name__ == "__main__":
    output_dir = os.getenv('DATA_PROCESSED_DIR', './data/processed')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'extracted_data.parquet')

    success = extract_to_parquet(output_path)
    print("Resultado:", "Éxito" if success else "Falló")