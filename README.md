# **Proyecto ETL para Carga y Consulta de Datos en MySQL**

![ETL Pipeline](https://img.shields.io/badge/ETL-Pipeline-blue) 
![Python](https://img.shields.io/badge/Python-3.11%2B-green)
![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-orange)
![Dificultad](https://img.shields.io/badge/Dificultad-Intermedio-yellow)

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** para cargar datos desde un archivo CSV (`data_prueba_tecnica.csv`) a una base de datos MySQL, con capacidades de consulta y transformación.

---

## **⚠️ Dificultades Clave y Soluciones**

### **1. Problemas con la Carga Inicial de Datos CSV**
#### **Dificultad Principal**:
- **Inconsistencias en el formato del CSV**: El archivo contenía valores nulos, formatos de fecha inconsistentes y tipos de datos mixtos en la columna `amount`.
- **Problemas de conexión con MySQL**: Errores de autenticación y configuración incorrecta del puerto.


---

### **2. Transformación de Datos Complejos**
#### **Dificultad Principal**:
- **Normalización de tablas**: Requería separar los datos en tablas relacionadas (`companies` y `charges`) manteniendo las relaciones.
- **Validación de estados**: Algunos valores en `status` no coincidían con los permitidos.


---

### **3. Creación de la Vista SQL Final**
#### **Dificultad Principal**:
- **Agregación diaria**: Calcular montos totales por día y compañía con JOIN entre tablas.
- **Problemas de rendimiento**: La vista era lenta con grandes volúmenes de datos.


---

## **📌 Requisitos**
- **Python 3.11+**
- **MySQL Server 8.0+**
- **Librerías**: `mysql-connector-python`, `pandas`, `python-dotenv`

---

## **⚙️ Configuración**
1. **Configurar `.env`**:
   ```ini
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=tu_contraseña
   DB_NAME=prueba_tecnica_db
   DATA_RAW_PATH=./data/raw/data_prueba_tecnica.csv
   ```

2. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

---

## **🚀 Flujo de Ejecución**
1. **Carga inicial**:
   ```bash
   python src/data_loader.py
   ```

2. **Transformación**:
   ```bash
   python src/data_transformer.py
   ```

3. **Normalización**:
   ```bash
   python src/schema_manager.py
   ```

4. **Consultar vista final**:
   ```bash
   python src/sql_queries.py
   ```


---

## **💡 Lecciones Aprendidas**
1. **Validar datos crudos antes de la carga**:
   - Implementar checks de calidad de datos en el CSV.
2. **Manejar conexiones a DB con contexto**:
   - Usar `with DatabaseManager() as db:` para evitar conexiones abiertas.
3. **Optimizar consultas SQL**:
   - Crear índices en columnas frecuentemente consultadas.
