import unittest
import pandas as pd
import os
from data_transformer import transform_data


class TestDataTransformer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Crear datos de prueba
        cls.test_data = pd.DataFrame({
            'id': ['1', '2', '3', ''],
            'company_id': ['c1', 'c2', 'c3', 'c4'],
            'company_name': ['Company 1', '', 'Company 3', None],
            'amount': ['100.50', '200', 'invalid', '300'],
            'status': ['completed', 'pending', 'failed', 'invalid'],
            'created_at': ['2023-01-01', '2023-01-02', 'invalid', '2023-01-04'],
            'updated_at': ['2023-01-01', '', None, '2023-01-04']
        })

        cls.test_input = 'test_input.parquet'
        cls.test_output = 'test_output.parquet'

        # Guardar datos de prueba
        cls.test_data.to_parquet(cls.test_input)

    def test_transform_data(self):
        # Ejecutar transformación
        result = transform_data(self.test_input, self.test_output)

        # Verificar que el archivo de salida existe
        self.assertTrue(os.path.exists(self.test_output))

        # Verificar que solo quedan 2 registros válidos (de 4 originales)
        self.assertEqual(len(result), 2)

        # Verificar tipos de datos
        self.assertIsInstance(result['amount'].iloc[0], float)
        self.assertIsInstance(result['created_at'].iloc[0], pd.Timestamp)

        # Verificar que company_name puede ser NULL
        self.assertTrue(pd.isna(result['company_name'].iloc[1]))

        # Verificar que status solo tiene valores permitidos
        valid_statuses = ['completed', 'pending', 'failed', 'refunded']
        self.assertTrue(all(result['status'].isin(valid_statuses)))

    @classmethod
    def tearDownClass(cls):
        # Limpiar archivos de prueba
        if os.path.exists(cls.test_input):
            os.remove(cls.test_input)
        if os.path.exists(cls.test_output):
            os.remove(cls.test_output)


if __name__ == '__main__':
    unittest.main()