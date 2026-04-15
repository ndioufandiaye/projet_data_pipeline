-- Table des clients (Référentiel)
CREATE TABLE IF NOT EXISTS customers (
    CustomerID INT PRIMARY KEY,
    Country TEXT
);

-- Table des ventes brutes (Ingérées depuis Kafka)
CREATE TABLE IF NOT EXISTS raw_sales (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID INT,
    Country TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de résumé quotidien pour le batch
CREATE TABLE IF NOT EXISTS daily_sales_summary (
    day DATE,
    country TEXT,
    total_revenue NUMERIC,
    total_quantity INT
);