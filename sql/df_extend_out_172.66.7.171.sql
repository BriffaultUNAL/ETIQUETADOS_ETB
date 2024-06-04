SELECT
    call_date,
    phone_number_dialed,
    campaign_id,
    status,
    status_name,
    user,
    list_id,
    length_in_sec,
    lead_id,
    uniqueid,
    caller_code,
    IP_DESCARGA,
    address1 as RF
FROM
    `bbdd_groupcos_repositorio_planta_telefonica`.`tb_marcaciones_vicidial_out_172.66.7.171`
WHERE
    campaign_id IN ('ETBCALLB' , 'ETBRECU', 'ETBRETEN', 'MGRA_PSE')
    AND (call_date between CURRENT_TIMESTAMP() - INTERVAL 1 DAY and CURRENT_TIMESTAMP()) 