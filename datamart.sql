-- Dimensions
CREATE TABLE dogservice.dim_product (
    product_id SERIAL PRIMARY KEY,
    procedure_name VARCHAR(100)
);

CREATE TABLE dogservice.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100)
);

-- Metrics
CREATE TABLE dogservice.fact_sales (
    sales_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES dogservice.dim_product(product_id),
    customer_id INT REFERENCES dogservice.dim_customer(customer_id),
    sales_date DATE,
    quantity INT,
    amount DECIMAL(10, 2)
);

-- Populate dim_product
INSERT INTO dogservice.dim_product (name)
SELECT DISTINCT name
FROM dogservice.procedure;

-- Populate dim_customer
INSERT INTO dogservice.dim_customer (customer_name, email)
SELECT DISTINCT name, email
FROM dogservice.customer;