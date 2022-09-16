CREATE TABLE IF NOT EXISTS prod_dv_stg.order_system_restaurants(
	object_id VARCHAR(24) UNIQUE NOT NULL,
	object_value TEXT	NOT NULL,
	update_ts TIMESTAMP	NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.order_system_orders(
	object_id VARCHAR(24) UNIQUE NOT NULL,
	object_value TEXT	NOT NULL,
	update_ts TIMESTAMP	NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.order_system_users(
	object_id VARCHAR(24) UNIQUE NOT NULL,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP	NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.bonus_system_users(
	bonus_user_id int UNIQUE NOT NULL,
	order_user_id	VARCHAR(24)	NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.bonus_system_outbox(
	event_type VARCHAR(20) NOT NULL,
	event_value	TEXT NOT NULL,
	event_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.couriers_system_couriers(
	_id	VARCHAR(24)	NOT NULL,
	name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.couriers_system_deliveries(
	order_id VARCHAR(24) NOT NULL,
	order_ts TIMESTAMP NOT NULL,
	delivery_id	VARCHAR(24)	NOT NULL,
	courier_id VARCHAR(24)	NOT NULL,
	address	VARCHAR(50)	NOT NULL,
	delivery_ts	TIMESTAMP NOT NULL,
	rate INT2 NOT NULL,
	sum NUMERIC(14,2) NOT NULL,
	tip_sum NUMERIC(8,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_dv_stg.mongo_etl_settings(
	id serial PRIMARY KEY,
	update_ts TIMESTAMP NOT NULL,
	table_name varchar(20) NOT NULL
);
