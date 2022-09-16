INSERT INTO prod_dv_dds.h_orders
(order_id_bk)
SELECT
	so.object_id AS order_id_bk
FROM 
	prod_dv_stg.order_system_orders AS so
LEFT JOIN
	prod_dv_dds.h_orders AS ddo
	ON so.object_id = ddo.order_id_bk 
WHERE 
	ddo.order_id_bk  IS NULL
	AND so.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
ON CONFLICT DO NOTHING;