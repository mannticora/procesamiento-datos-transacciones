# **Proyecto ETL para Carga y Consulta de Datos en MySQL**

![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-blue) 
![Python](https://img.shields.io/badge/Python-3.11%2B-green)
![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-orange)

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** para cargar datos desde un archivo CSV (`data_prueba_tecnica.csv`) a una base de datos MySQL, con capacidades de consulta y transformación.

---

## **📌 Requisitos**
- **Python 3.11+**
- **MySQL Server 8.0+** (local o remoto)
- Librerías listadas en `requirements.txt`

---

## **⚙️ Configuración Inicial**
### 1. **Clonar el repositorio**
```bash
git clone https://github.com/tu-usuario/proyecto-etl-mysql.git
cd proyecto-etl-mysql
```

### 2. **Configurar variables de entorno**
Crea un archivo `.env` en la raíz del proyecto con:
```ini
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=tu_contraseña_mysql
DB_NAME=prueba_tecnica_db
DATA_RAW_PATH=./data/raw/data_prueba_tecnica.csv
```

### 3. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

---

## **🚀 Ejecución del Proyecto**
### **Carga inicial de datos**
```bash
python src/data_loader.py
```
> Carga el archivo CSV a una tabla temporal `raw_data` en MySQL.

### **Transformación y normalización**
```bash
python src/schema_manager.py
```
> Crea las tablas normalizadas (`companies` y `charges`) y carga los datos transformados.

### **Consultas personalizadas**
```bash
python src/sql_queries.py
```
> Ejecuta consultas predefinidas (ej: vista de transacciones diarias).

## **🔍 Consultas de Ejemplo**
### Desde Python:
```python
from src.database import DatabaseManager

with DatabaseManager() as db:
    # Top 5 compañías con más transacciones
    result = db.execute_query("""
        SELECT company_id, COUNT(*) AS total_transactions
        FROM charges
        GROUP BY company_id
        ORDER BY total_transactions DESC
        LIMIT 5
    """, fetch=True)
    print(result)
```

### Directamente en MySQL:
```sql
-- Monto total por día
SELECT * FROM daily_company_transactions;
```

---

## **🛠️ Troubleshooting**
| **Error**                     | **Solución**                                  |
|-------------------------------|-----------------------------------------------|
| `Access denied for user`      | Verifica `.env` y permisos de MySQL          |
| `CSV not found`               | Asegúrate de que la ruta en `.env` es correcta |
| `ModuleNotFoundError`         | Ejecuta `pip install -r requirements.txt`    |

---

## **📄 Licencia
Este proyecto está bajo la licencia [MIT](LICENSE).

---
