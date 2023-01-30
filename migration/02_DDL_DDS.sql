-- HUBS

CREATE TABLE IF NOT EXISTS prod_dv_dds.h_restaraunts(
	restaraunt_id_dwh serial PRIMARY KEY,
	restaraunt_id_bk VARCHAR(24) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.h_products(
	product_id_dwh serial PRIMARY KEY,
	product_id_bk VARCHAR(24) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.h_users(
	user_id_dwh serial PRIMARY KEY,
	user_id_bk VARCHAR(24) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.h_couriers(
	courier_id_dwh serial PRIMARY KEY,
	courier_id_bk VARCHAR(24) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.h_orders(
	order_id_dwh serial PRIMARY KEY,
	order_id_bk VARCHAR(24) UNIQUE NOT NULL
);

-- LINKS

CREATE TABLE IF NOT EXISTS prod_dv_dds.l_orders(
	order_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_orders(order_id_dwh),
	restaraunt_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_restaraunts(restaraunt_id_dwh),
	user_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_users(user_id_dwh),
	courier_id_dwh INTEGER NULL REFERENCES prod_dv_dds.h_couriers(courier_id_dwh),
	product_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_products(product_id_dwh),
	quantity INTEGER NOT NULL CHECK (quantity > 0) DEFAULT 1,
	bonus_payment NUMERIC(8,2) NOT NULL CHECK (bonus_payment >= 0) DEFAULT 0,
	bonus_grant NUMERIC(8,2) NOT NULL CHECK (bonus_grant >= 0) DEFAULT 0,
	CONSTRAINT l_orders_unique UNIQUE (order_id_dwh, product_id_dwh)
);

-- SATELLITES


CREATE TABLE IF NOT EXISTS prod_dv_dds.s_restaraunts(
	restaraunt_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_restaraunts(restaraunt_id_dwh),
	restaraunt_name VARCHAR(100) NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.s_products(
	product_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_products(product_id_dwh),
	product_name VARCHAR(100) NOT NULL,
	product_price NUMERIC(8,2) NOT NULL CHECK (product_price >= 0) DEFAULT 0,
	product_category VARCHAR(100) NOT NULL DEFAULT 'General',
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.s_users(
	user_id_dwh INTEGER NOT NULL REFERENCES prod_dv_dds.h_users(user_id_dwh),
	user_name VARCHAR(100) NOT NULL,
	user_login VARCHAR(25) NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL	
);


CREATE TABLE IF NOT EXISTS prod_dv_dds.s_couriers(
	courier_id_dwh INTEGER NULL REFERENCES prod_dv_dds.h_couriers(courier_id_dwh),
	courier_name VARCHAR(100) NOT NULL,
	active_from TIMESTAMP NULL,
	active_to TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_dds.s_orders(
	order_id_dwh INTEGER PRIMARY KEY REFERENCES prod_dv_dds.h_orders(order_id_dwh),
	order_tip_sum NUMERIC(8,2) NULL CHECK (order_tip_sum >= 0) DEFAULT 0,
	order_rate INT2 NOT NULL CHECK (order_rate BETWEEN 1 AND 5) DEFAULT 5,
	order_status VARCHAR(9) NOT NULL,
	order_address VARCHAR(50) NULL,
	order_create_ts TIMESTAMP NOT NULL,
	order_deliviring_ts TIMESTAMP NULL	
);

