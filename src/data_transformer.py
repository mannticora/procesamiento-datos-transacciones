import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


def transform_data(input_path: str, output_path: str) -> bool:
    try:
        # Leer datos
        df = pd.read_parquet(input_path)
        print(f"Transformando {len(df)} registros...")

        # 1. Limpieza de IDs
        df = df[df['id'].notna() & (df['id'] != '')]

        # 2. Limpieza de company_name
        df['company_name'] = df['company_name'].replace(['', 'null', 'NULL'], None)

        # 3. Validación de company_id
        df = df[df['company_id'].notna() & (df['company_id'] != '')]

        # 4. Conversión de amount
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'].notna()]

        # 5. Validación de status
        valid_statuses = ['completed', 'pending', 'failed', 'refunded']
        df = df[df['status'].isin(valid_statuses)]

        # 6. Conversión de fechas
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df = df[df['created_at'].notna()]
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')

        # 7. Selección de columnas finales
        final_df = df[[
            'id', 'company_name', 'company_id',
            'amount', 'status', 'created_at', 'updated_at'
        ]]

        # Guardar resultado
        final_df.to_parquet(output_path, index=False)
        print(f"Datos transformados guardados en {output_path}")
        return True

    except Exception as e:
        print(f"Error transformando datos: {e}")
        return False


if __name__ == "__main__":
    input_dir = os.getenv('DATA_PROCESSED_DIR', './data/processed')
    input_path = os.path.join(input_dir, 'extracted_data.parquet')
    output_path = os.path.join(input_dir, 'transformed_data.parquet')

    success = transform_data(input_path, output_path)
    print("Resultado:", "Éxito" if success else "Falló")