-- query for update changes
WITH update_news AS
	(SELECT 
		dhr.restaraunt_id_dwh AS restaraunt_id_dwh,
		sr.object_value::JSON->>'name' AS restaraunt_name,
		sr.update_ts AS update_ts 
	FROM 
		prod_dv_stg.order_system_restaurants AS sr
	JOIN
		prod_dv_dds.h_restaraunts AS dhr 
			ON sr.object_id = dhr.restaraunt_id_bk
	LEFT JOIN 
		prod_dv_dds.s_restaraunts AS dsr 
			ON dhr.restaraunt_id_dwh = dsr.restaraunt_id_dwh
	WHERE 
		(sr.object_value::JSON->>'name') != dsr.restaraunt_name 
		AND dsr.active_to = '2099-12-01'::timestamp
		AND sr.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
	)
UPDATE prod_dv_dds.s_restaraunts AS dsr
SET active_to = un.update_ts - INTERVAL '1 ms'
FROM update_news AS un
WHERE dsr.restaraunt_id_dwh = un.restaraunt_id_dwh AND dsr.active_to = '2099-12-01'::timestamp;

--query to insert new
INSERT INTO prod_dv_dds.s_restaraunts
(restaraunt_id_dwh, restaraunt_name, active_from, active_to)
SELECT
	dhr.restaraunt_id_dwh AS restaraunt_id_dwh,
	sr.object_value::JSON->>'name' AS restaraunt_name,
	sr.update_ts AS active_from,
	'2099-12-01'::timestamp AS active_to
FROM 
	prod_dv_stg.order_system_restaurants AS sr
JOIN
	prod_dv_dds.h_restaraunts AS dhr 
		ON sr.object_id = dhr.restaraunt_id_bk
-- compare only last values
LEFT JOIN 
	(SELECT
		DISTINCT FIRST_VALUE(restaraunt_id_dwh) OVER actual_value AS restaraunt_id_dwh,
		FIRST_VALUE(restaraunt_name) OVER actual_value AS restaraunt_name
	FROM
		prod_dv_dds.s_restaraunts
	WINDOW actual_value AS (PARTITION BY restaraunt_id_dwh ORDER BY active_to DESC)) AS dsr
		ON dhr.restaraunt_id_dwh = dsr.restaraunt_id_dwh 
WHERE 
	((sr.object_value::JSON->>'name') != dsr.restaraunt_name  OR dsr.restaraunt_name IS NULL)
	AND sr.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}';

