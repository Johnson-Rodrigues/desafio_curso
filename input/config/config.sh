#!/bin/bash

DATE="$(date --date="-0 day" "+%Y%m%d")"



TABLES=("clientes" "divisao" "endereco" "regiao" "vendas")

PARTICAO="$(date --date="-0 day" "+%Y%m%d")"