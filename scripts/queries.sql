CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id serial PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone_number VARCHAR(24) NOT NULL,
    curp VARCHAR(18) NOT NULL,
    rfc VARCHAR(13) NOT NULL,
    address VARCHAR(250) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.items (
    item_id serial PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    price NUMERIC (5, 2)
);

CREATE TABLE IF NOT EXISTS staging.stores (
    store_id serial PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    location NUMERIC VARCHAR(100) NOT NULL
);

INSERT INTO staging.customers (first_name, last_name, phone_number, curp, rfc, address) VALUES
    ('JUAN', 'ESPINOZA', '+1 212 555 0188', 'EIRJ720502HTSSDN06', 'EIRJ720502959', '2014 Jabberwocky Rd'),
    ('MAGDALENA', 'GONZALEZ', '+61 3 7010 4321', 'GORM680121MTSNDG06', 'GORM680121245', '2011 Interiors Blvd'),
    ('TERESO', 'ALVAREZ', '+61 491 578 888', 'AALT601015HTSLPR00', 'AALT601015978', '2004 Charade Rd'),
    ('RODOLFO', 'GAMBOA', '+44 20 7946 0990', 'GACR450423HSPMMD06', 'GACR450423923', '147 Spadina Ave'),
    ('MAXIMINO', 'CASTILLO', '+61 2 7010 0000', 'CACM700821HTSSSX04', 'CACM700821482', '8204 Arthur St'),
    ('JUVENTINO', 'ACUÑA', '+61 491 570 156', 'CAAJ880203HTSSCV13', 'CAAJ880203471', 'Magdalen Centre'),
    ('ANTONIO', 'SALINAS', '+1 908-204-0495', 'SACA920422HTSLSN02', 'SACA920422463', 'Schwanthalerstr. 7031'),
    ('LAURO', 'MORALES', '+1 801-752-2367', 'EIPS670504HTSSZB03', 'EIPS670504253', '54, rue Royale'),
    ('BENITO', 'CABRIALES', '+1 608-299-8640', 'CAGB880925HTSBNN15', 'CAGB880925101', 'Berguvsvägen 8'),
    ('LOURDES', 'ROBLEDO', '+1 870-381-6967', 'FIRL720110MTSSBR03', 'FIRL720110869', 'Rambla de Cataluña, 23');

INSERT INTO staging.items (name, price) VALUES
    ('Teatime Chocolate Biscuits', 103.42),
    ('Mishi Kobe Niku', 77.90),
    ('Queso Manchego La Pastora', 55.70),
    ('Gravad lax', 118.50),
    ('Ipoh Coffee', 122.73),
    ('Filo Mix', 33.30),
    ('Ravioli Angelo', 56.76),
    ('Perth Pasties', 24.92),
    ('Mozzarella di Giovanni', 84.48),
    ('Stroopwafels', 62.46);

INSERT INTO staging.stores (name, location) VALUES
    ('Lyon Souveniers', '-56.03384337, 88.5825877'),
    ('Rovelli Gifts', '18.18294, -77.91013'),
    ('Schuyler Imports', '4.43363, 50.97070'),
    ('Der Hund Imports', '-55.39533, 175.02882'),
    ('Suominen Souveniers', '-46.40251, 64.26651'),
    ('Asian Treasures', '36.66938, 80.78203'),
    ('Royale Belge', '55.05569, 147.05592'),
    ('Cruz & Sons Co.', '10.84176, 36.05627'),
    ('Feuer Online Stores', '35.55883, 74.55527'),
    ('Warburg Exchange', '47.83025, -119.37847');