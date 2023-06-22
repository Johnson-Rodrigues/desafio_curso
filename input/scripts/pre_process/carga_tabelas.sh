#!/bin/bash
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"

TABLES=("clientes" "divisao" "endereco" "regiao" "vendas")

for table in "${TABLES[@]}"
do
    TARGET_DATABASE="desafio_curso"
    HDFS_DIR="/datalake/raw/$table"
    TARGET_TABLE_EXTERNAL="$table"
    TARGET_TABLE_GERENCIADA="tbl_$table"

    beeline -u jdbc:hive2://localhost:10000 \
    --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
    --hivevar HDFS_DIR="${HDFS_DIR}"\
    --hivevar TARGET_TABLE_EXTERNAL="${TARGET_TABLE_EXTERNAL}"\
    --hivevar TARGET_TABLE_GERENCIADA="${TARGET_TABLE_GERENCIADA}"\
    --hivevar PARTICAO="${PARTICAO}"\
    -f ../../hql/create_table_$table.hql 
done

  