INSERT INTO prod_dv_dds.h_users
(user_id_bk)
SELECT
	su.object_id  AS user_id_bk
FROM 
	prod_dv_stg.order_system_users AS su
LEFT JOIN
	prod_dv_dds.h_users AS du
		ON su.object_id = du.user_id_bk 
WHERE 
	du.user_id_bk IS NULL
	AND su.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
ON CONFLICT DO NOTHING;