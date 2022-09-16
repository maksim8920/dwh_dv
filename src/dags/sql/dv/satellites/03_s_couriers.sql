DELETE FROM prod_dv_dds.s_couriers;

INSERT INTO prod_dv_dds.s_couriers
(courier_id_dwh, courier_name, active_from, active_to)
SELECT 
	dc.courier_id_dwh AS courier_id_dwh,
	sc."name" AS courier_name,
	NULL AS active_from,
	NULL AS active_to
FROM 
	prod_dv_stg.couriers_system_couriers AS sc
JOIN
	prod_dv_dds.h_couriers AS dc
		ON sc."_id" = dc.courier_id_bk;