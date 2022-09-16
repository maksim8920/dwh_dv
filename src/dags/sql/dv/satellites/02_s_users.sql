-- query for update changes
WITH update_news AS
	(SELECT 
		dhu.user_id_dwh AS user_id_dwh,
		su.object_value::JSON->>'name' AS user_name,
		su.object_value::JSON->>'login' AS user_login,
		su.update_ts AS update_ts
	FROM 
		prod_dv_stg.order_system_users AS su
	JOIN
		prod_dv_dds.h_users AS dhu
			ON su.object_id = dhu.user_id_bk 
	LEFT JOIN 
		prod_dv_dds.s_users AS dsu 
			ON dhu.user_id_dwh = dsu.user_id_dwh
	WHERE 
		((su.object_value::JSON->>'name') != dsu.user_name OR su.object_value::JSON->>'login' != dsu.user_login)
		AND dsu.active_to = '2099-12-01'::timestamp
		AND su.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}')
UPDATE prod_dv_dds.s_users AS dsu
SET active_to = un.update_ts - INTERVAL '1 ms'
FROM update_news AS un
WHERE dsu.user_id_dwh = un.user_id_dwh AND dsu.active_to = '2099-12-01'::timestamp;

--query to insert new
INSERT INTO prod_dv_dds.s_users
(user_id_dwh, user_name, user_login, active_from, active_to)
SELECT 
	dhu.user_id_dwh AS user_id_dwh,
	su.object_value::JSON->>'name' AS user_name,
	su.object_value::JSON->>'login' AS user_login,
	su.update_ts AS active_from,
	'2099-12-01'::timestamp AS active_to
FROM 
	prod_dv_stg.order_system_users AS su
JOIN
	prod_dv_dds.h_users AS dhu
		ON su.object_id = dhu.user_id_bk 
LEFT JOIN 
	(SELECT
		DISTINCT FIRST_VALUE(user_id_dwh) OVER actual_value AS user_id_dwh,
		FIRST_VALUE(user_name) OVER actual_value AS user_name,
		FIRST_VALUE(user_login) OVER actual_value AS user_login
	FROM
		prod_dv_dds.s_users
	WINDOW actual_value AS (PARTITION BY user_id_dwh ORDER BY active_to DESC)) AS dsu
		ON dhu.user_id_dwh = dsu.user_id_dwh
WHERE 
	((su.object_value::JSON->>'name') != dsu.user_name OR su.object_value::JSON->>'login' != dsu.user_login OR dsu.user_name IS NULL)
	AND su.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}';