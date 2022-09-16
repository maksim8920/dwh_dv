INSERT INTO prod_dv_dds.h_restaraunts
(restaraunt_id_bk)
SELECT
	sr.object_id AS restaraunt_id_bk	
FROM 
	prod_dv_stg.order_system_restaurants AS sr
LEFT JOIN
	prod_dv_dds.h_restaraunts AS dr 
		ON sr.object_id = dr.restaraunt_id_bk 
WHERE 
	dr.restaraunt_id_bk IS NULL
	AND sr.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
ON CONFLICT DO NOTHING;
