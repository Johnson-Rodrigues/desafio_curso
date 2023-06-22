CREATE EXTERNAL TABLE IF NOT  EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
    id_address_number string,
    ds_city string,
    id_country string,
    ds_costumer_adress_1 string,
    ds_costumer_adress_2 string,
    ds_costumer_adress_3 string,
    ds_costumer_adress_4 string,
    id_state string,
    ds_zip_code string
    )
COMMENT 'Tabela de Endereço'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    id_address_number string,
    ds_city string,
    id_country string,
    ds_costumer_adress_1 string,
    ds_costumer_adress_2 string,
    ds_costumer_adress_3 string,
    ds_costumer_adress_4 string,
    id_state string,
    ds_zip_code string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY');


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA}
PARTITION(DT_FOTO) 
SELECT
    id_address_number string,
    ds_city string,
    id_country string,
    ds_costumer_adress_1 string,
    ds_costumer_adress_2 string,
    ds_costumer_adress_3 string,
    ds_costumer_adress_4 string,
    id_state string,
    ds_zip_code string,
	${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;
