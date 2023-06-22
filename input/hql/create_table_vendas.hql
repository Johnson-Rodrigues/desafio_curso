CREATE EXTERNAL TABLE IF NOT  EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
    ds_actual_delivery_date string,
    id_costumer_key string,
    ds_invoice_date string,
    ds_discount_amount string,
    id_invoice_number string,
    id_item_class string,
    id_item_number string,
    ds_item string,
    ds_line_number string,
    ds_list_prince string,
    id_order_number string,
    ds_promised_delivery_date string,
    ds_sales_amount string,
    ds_sales_amount_list_prince string,
    ds_sales_cost_amount string,
    ds_sales_margin_amount string,
    ds_sales_price string,
    ds_sales_quantity string,
    id_sales_rep string,
    ds_um string,
    ds_dado1 string,
    ds_dado2 string,
    ds_dado3 string,
    ds_dado4 string,
    ds_dado5 string,
    ds_dado6 string
    )
COMMENT 'Tabela de Vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    ds_actual_delivery_date string,
    id_costumer_key string,
    ds_invoice_date string,
    ds_discount_amount string,
    id_invoice_number string,
    id_item_class string,
    id_item_number string,
    ds_item string,
    ds_line_number string,
    ds_list_prince string,
    id_order_number string,
    ds_promised_delivery_date string,
    ds_sales_amount string,
    ds_sales_amount_list_prince string,
    ds_sales_cost_amount string,
    ds_sales_margin_amount string,
    ds_sales_price string,
    ds_sales_quantity string,
    id_sales_rep string,
    ds_um string,
    ds_dado1 string,
    ds_dado2 string,
    ds_dado3 string,
    ds_dado4 string,
    ds_dado5 string,
    ds_dado6 string
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
    ds_actual_delivery_date string,
    id_costumer_key string,
    ds_invoice_date string,
    ds_discount_amount string,
    id_invoice_number string,
    id_item_class string,
    id_item_number string,
    ds_item string,
    ds_line_number string,
    ds_list_prince string,
    id_order_number string,
    ds_promised_delivery_date string,
    ds_sales_amount string,
    ds_sales_amount_list_prince string,
    ds_sales_cost_amount string,
    ds_sales_margin_amount string,
    ds_sales_price string,
    ds_sales_quantity string,
    id_sales_rep string,
    ds_um string,
    ds_dado1 string,
    ds_dado2 string,
    ds_dado3 string,
    ds_dado4 string,
    ds_dado5 string,
    ds_dado6 string,
	${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;
