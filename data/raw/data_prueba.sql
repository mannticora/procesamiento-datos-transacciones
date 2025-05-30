CREATE TABLE charges (
    id VARCHAR(24) NOT NULL,
    company_name VARCHAR(130),
    company_id VARCHAR(24) NOT NULL,
    amount DECIMAL(16,2) NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    PRIMARY KEY (id)
);