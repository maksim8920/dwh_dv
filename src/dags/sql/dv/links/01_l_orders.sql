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
		(so.object_value::JSON->>'date')::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}')
DELETE FROM prod_dv_dds.l_orders WHERE order_id_dwh IN (SELECT order_id_dwh FROM drop_orders);

-- insert values
INSERT INTO prod_dv_dds.l_orders
(order_id_dwh, restaraunt_id_dwh, user_id_dwh, courier_id_dwh, product_id_dwh, quantity, bonus_payment, bonus_grant)
SELECT 
	ddo.order_id_dwh AS order_id_dwh,
	dr.restaraunt_id_dwh AS restaraunt_id_dwh,
	du.user_id_dwh AS user_id_dwh,
	dc.courier_id_dwh AS courier_id_dwh,
	dp.product_id_dwh AS product_id_dwh,
	toi.quantity AS quantity,
	tob.bonus_payment AS bonus_payment,
	tob.bonus_grant AS bonus_grant
FROM 
	prod_dv_stg.order_system_orders AS so
JOIN
	prod_dv_dds.h_orders AS ddo 
		ON so.object_id = ddo.order_id_bk 
JOIN 
	prod_dv_dds.h_restaraunts AS dr 
		ON (so.object_value::JSON->>'restaurant')::JSON->>'id' = dr.restaraunt_id_bk 
JOIN 
	prod_dv_dds.h_users AS du 
		ON (so.object_value::JSON->>'user')::JSON->>'id' = du.user_id_bk
JOIN 
	public.temp_order_items AS toi 
		ON so.object_id = toi.order_id
JOIN 
	prod_dv_dds.h_products AS dp 
		ON toi.product_id = dp.product_id_bk
JOIN 
	public.temp_order_bonuses AS tob 
		ON so.object_id = tob.order_id
		AND toi.product_id = tob.product_id 
LEFT JOIN 
	prod_dv_stg.couriers_system_deliveries AS sd 
		ON sd.order_id = so.object_id 
LEFT JOIN 
	prod_dv_dds.h_couriers AS dc 
		ON sd.courier_id = dc.courier_id_bk 
WHERE 
	(so.object_value::JSON->>'date')::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}';
