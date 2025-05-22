from src.data_loader import load_raw_data
import os
import pytest

@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "test_data.csv"
    csv_path.write_text("""id,company_id,company_name,amount,status,created_at,updated_at
1,C1,Company 1,100.50,completed,2023-01-01,2023-01-01
2,C2,Company 2,200.00,pending,2023-01-02,2023-01-02""")
    return str(csv_path)

def test_load_raw_data(sample_csv):
    os.environ['DATA_RAW_PATH'] = sample_csv
    assert load_raw_data(sample_csv) is True