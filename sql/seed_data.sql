INSERT INTO customers (CustomerID, Country) VALUES
(17850, 'United Kingdom'),
(13047, 'United Kingdom'),
(12583, 'France')
ON CONFLICT (CustomerID) DO NOTHING;