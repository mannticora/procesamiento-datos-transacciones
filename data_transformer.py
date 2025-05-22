import pandas as pd
from datetime import datetime


def transform_data(input_file, output_file):
    """

    Args:
        input_file (str): C:\Users\ricar\OneDrive\Escritorio\data_prueba_tecnica.csv
        output_file (str): C:\Users\ricar\OneDrive\Escritorio
    """
    try:
        # Leer datos
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        else:
            df = pd.read_csv(input_file)

        # 1. Verificar y limpiar la columna 'id'
        df['id'] = df['id'].str.strip()
        df = df[df['id'].notna() & (df['id'] != '')]

        # 2. Limpiar company_name (puede ser NULL)
        df['company_name'] = df['company_name'].replace('', None)

        # 3. Verificar company_id
        df = df[df['company_id'].notna() & (df['company_id'] != '')]

        # 4. Transformar amount a decimal
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'].notna()]

        # 5. Validar status
        valid_statuses = ['completed', 'pending', 'failed', 'refunded']
        df = df[df['status'].isin(valid_statuses)]

        # 6. Convertir fechas a timestamp
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df = df[df['created_at'].notna()]

        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')

        # 7. Seleccionar solo las columnas necesarias en el orden correcto
        final_df = df[[
            'id', 'company_name', 'company_id',
            'amount', 'status', 'created_at', 'updated_at'
        ]]

        # Guardar datos transformados
        final_df.to_parquet(output_file, index=False)
        print(f"Datos transformados guardados en {output_file}")

        return final_df

    except Exception as e:
        print(f"Error durante la transformación: {e}")
        raise


if __name__ == "__main__":
    transform_data('extracted_data.parquet', 'transformed_data.parquet')