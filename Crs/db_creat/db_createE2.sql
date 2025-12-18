SET client_encoding TO 'UTF8';
SET standard_conforming_strings TO on;
SET session_replication_role = replica; 


CREATE TABLE clients (
    client_id TEXT PRIMARY KEY,
    client_prsn_id TEXT NOT NULL,
    client_post_index TEXT NOT NULL,
    client_city TEXT,
    client_state TEXT
);

CREATE TABLE sellers (
    seller_id TEXT PRIMARY KEY,
    seller_post_index TEXT NOT NULL,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_categoru TEXT NOT NULL,
    product_name TEXT NOT NULL,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT
);

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL REFERENCES clients(client_id), 
    order_status TEXT NOT NULL,
    purchase_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    approved_at TIMESTAMP WITHOUT TIME ZONE,
    delivered_at TIMESTAMP WITHOUT TIME ZONE,
    estimated_delivery TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE order_items (
    order_id TEXT NOT NULL REFERENCES orders(order_id), 
    item_id INT NOT NULL,
    product_id TEXT NOT NULL REFERENCES products(product_id), 
    seller_id TEXT NOT NULL REFERENCES sellers(seller_id), 
    price NUMERIC(10, 2) NOT NULL,
    freight_value NUMERIC(10, 2) NULL,
    PRIMARY KEY (order_id, item_id)
);

CREATE INDEX idx_order_items_product_id ON order_items (product_id);
CREATE INDEX idx_order_items_seller_id ON order_items (seller_id);
CREATE INDEX idx_orders_client_id ON orders (client_id);
CREATE INDEX idx_orders_status_date ON orders (order_status, purchase_date);
CREATE INDEX idx_orders_purchase_date ON orders (purchase_date);
CREATE INDEX idx_orders_purchase_date_id ON orders (purchase_date, order_id);
CREATE INDEX idx_oi_product_price ON order_items (product_id, price, order_id);
CREATE INDEX idx_products_category ON products (product_categoru);