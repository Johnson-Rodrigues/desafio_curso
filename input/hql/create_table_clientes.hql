CREATE EXTERNAL TABLE IF NOT  EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
    id_adress string,
    ds_business_family string,
    id_business_unit string,
    ds_costumer string,
    id_costumer_key string,
    ds_costumer_type string,
    id_division string,
    ds_line_business string,
    id_phone string,
    id_region_code string,
    ds_sales_mgr string,
    id_search_type string
    )
COMMENT 'Tabela de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    id_adress string,
    ds_business_family string,
    id_business_unit string,
    ds_costumer string,
    id_costumer_key string,
    ds_costumer_type string,
    id_division string,
    ds_line_business string,
    id_phone string,
    id_region_code string,
    ds_sales_mgr string,
    id_search_type string
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
    id_adress string,
    ds_business_family string,
    id_business_unit string,
    ds_costumer string,
    id_costumer_key string,
    ds_costumer_type string,
    id_division string,
    ds_line_business string,
    id_phone string,
    id_region_code string,
    ds_sales_mgr string,
    id_search_type string,
	${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;
