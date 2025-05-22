from src.database import DatabaseManager
import pytest

def test_db_connection():
    with DatabaseManager() as db:
        assert db.connect() is True
        assert db.connection.is_connected() is True
        version = db.execute_query("SELECT VERSION()", fetch=True)
        assert 'MySQL' in version[0]['VERSION()']