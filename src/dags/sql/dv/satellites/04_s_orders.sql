-- idempotency
WITH drop_orders as
	(SELECT 
		ddo.order_id_dwh  
	FROM 
		prod_dv_stg.order_system_orders AS so
	JOIN
		prod_dv_dds.h_orders AS ddo 
			ON so.object_id = ddo.order_id_bk 
	WHERE 
		(so.object_value::JSON->>'date')::date BETWEEN '{{ dag.timezone.convert(yesterday_ds).strftime("%Y-%m-%d") }}' AND '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}')
DELETE FROM prod_dv_dds.s_orders WHERE order_id_dwh IN (SELECT order_id_dwh FROM drop_orders);

-- insert rows
INSERT INTO prod_dv_dds.s_orders
(order_id_dwh, order_tip_sum, order_rate, order_status, order_address, order_create_ts, order_deliviring_ts)
SELECT 
	ddo.order_id_dwh AS order_id_dwh,
	sd.tip_sum AS order_tip_sum,
	sd.rate AS order_rate,
	so.object_value::JSON->>'final_status' AS order_status,
	sd.address AS order_address,
	(so.object_value::JSON->>'date')::timestamp AS order_create_ts,
	sd.delivery_ts AS order_deliviring_ts
FROM 
	prod_dv_stg.order_system_orders AS so
JOIN 
	prod_dv_dds.h_orders AS ddo 
		ON so.object_id = ddo.order_id_bk 
LEFT JOIN
	prod_dv_stg.couriers_system_deliveries AS sd 
		ON so.object_id = sd.order_id 
WHERE
	(so.object_value::JSON->>'date')::date BETWEEN '{{ dag.timezone.convert(yesterday_ds).strftime("%Y-%m-%d") }}' AND '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}');

		
