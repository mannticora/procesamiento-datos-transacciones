-- Tabla de compañías
CREATE TABLE IF NOT EXISTS companies (
    company_id VARCHAR(24) PRIMARY KEY,
    company_name VARCHAR(130)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabla de cargos
CREATE TABLE IF NOT EXISTS charges (
    id VARCHAR(24) PRIMARY KEY,
    company_id VARCHAR(24) NOT NULL,
    amount DECIMAL(16,2) NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;