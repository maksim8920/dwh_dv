INSERT INTO prod_dv_dds.h_couriers
(courier_id_bk)
SELECT
	sc."_id" AS courier_id_bk
FROM 
	prod_dv_stg.couriers_system_couriers AS sc
LEFT JOIN
	prod_dv_dds.h_couriers AS dc 
		ON sc."_id" = dc.courier_id_bk 
WHERE
    dc.courier_id_bk IS NULL
ON CONFLICT DO NOTHING;