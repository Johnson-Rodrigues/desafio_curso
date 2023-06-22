# desafio_curso
Projeto para o Curso de Big Data Minsait

Criei um workspace em GITPOD.IO. 
Adicionei a extensão DOCKER clicando em EXTENSÕES dentro do codespaces.
Importei o contêiner já pronto que estava em https://github.com/caiuafranca/bigdata_docker
com o comando git clone -b ambiente-curso https://github.com/caiuafranca/bigdata_docker

Mudei para a pasta bigdata_docker com o comando: cd bigdata_docker
Não inibi nenhum contêiner. Então subi os contêineres com o comando: docker-compose up -d

Com o comando linux ls verifiquei a existência da pasta input que fui criada com a execução do docker.

Dei permissão total para a pasta input com: sudo chmod 777 input

Foi até a pasta input que foi criada quando o docker foi executado.

Criei a estrutura de pastas como pedida, assim: 
input
	desafio_curso
		app
		gold
		raw
		scripts
			pre_process

Não foi necessário dar download dos arquivos pois eles já foram enviados juntos com o desafio então foi só carregar esses arquivos para as pastas respectivas em raw.
Então dentro da pasta raw foram criadas as pastas: clientes, divisao, endereco, regiao, vendas. E foram copiados para lá os respectivos arquivos fornecidos junto com o desafio: 
 - VENDAS.CSV
 - CLIENTES.CSV
 - ENDERECO.CSV
 - REGIAO.CSV
- DIVISAO.CSV

Após isso foi criada a pasta hlq dentro de desafio_curso, esta pasta conterá os arquivos hql para ingestão de dados no Banco de dados.

Foram criados os arquivos hql para ingestão:
create_table_clientes;
create_table_divisao;
create_table_endereco;
create_table_regiao;
create_table_vendas.

Durante a criação dos arquivos hql tive que olhar a estrutura dos arquivos e o arquivo vendas tinha 6 colunas sem denominação alguma, apenas os dados, então os nomeie como ds_dado1, ds_dado2,     ds_dado3, ds_dado4, ds_dado5 e ds_dado6; pois o cliente pode sentir a necessidade e pensar alguma maneira desses dados fazerem algum sentido para ele.

Foi criado o script na pasta scripts/pre_process para ingestão de dados no banco desafio_curso: carga_tabelas.sh. Esse arquivo fará a chamada automática dos arquivos hql para carga das tabelas no banco.

Executei o contêiner do namenode para criar as pastas do datalake: raw, silver e gold. (docker exec -it namenode bash) E após isso executei os comandos para a criação das pastas:

hdfs dfs -mkdir /datalake/
 hdfs dfs -mkdir /datalake/raw
 hdfs dfs -mkdir /datalake/silver
 hdfs dfs -mkdir /datalake/gold

Fiz um arquivo chamado de carga_datalake.sh  que é responsável por transferir os dados da pasta raw para o datalake.

Também criei a pasta config embaixo da pasta desafio_curso que contem o arquivo config.sh com as configurações iniciais para rodar os scripts da pasta pre_process.

Dei a permissão de execução para os scripts da pasta pre_process:  carga_datalake.sh e  carga_tabelas.sh, bem como para o config.sh da pasta config. Isso foi feito usando o comando chmod +x. 

Rodei o script carga_datalake.sh no namenode (docker exec -it namenode bash) e os arquivos foram para o hdfs.

Executei o HIVE com docker exec -it hive-server bash

Já dentro do HIVE executei o comando para rodar o beeline:
beeline -u jdbc:hive2://localhost:10000

Criei o banco de dados desafio_curso com o comando: create database desafio_curso;

Ainda dentro do HIVE, rodei o script  carga_tabelas.sh para a carga das tabelas no banco de dados desafio_curso.

Verificando no beeline verifiquei que as tabelas foram criadas, como a seguir:
+---------------+
|   tab_name    |
+---------------+
| clientes      |
| divisao       |
| endereco      |
| regiao        |
| tbl_clientes  |
| tbl_divisao   |
| tbl_endereco  |
| tbl_regiao    |
| tbl_vendas    |
| vendas        |


Entrei no Jupyter notebook para desenvolver o programa process.py para tratamento de dados em usando o Spark.


Em process.py criei as dimensões VENDAS e TEMPO; e a tabela FATO VENDAS que irá responder às perguntas de negócio.

Não achei relacionamento entre as tabelas ENDERECO, REGIAO e DIVISÃO e por isso foram desconsideradas.

Em process.py foram criadas as tabelas na pasta gold: dim_tempo.csv, dim_vendas.csv e ft_vendas.csv, que serão consumidas pelo POWER BI.

Baixei na minha máquina local as tabelas criadas e importei para o POWER BI.

Quando verifiquei as tabelas no POWERBI, percebi que os dados das colunas ds_discount_amount e ds_invoice_date estavam invertidos e precisei voltar para arrumar esses dados, pois senão não conseguiria resolver as questões pedidas.

Com esse contratempo voltei e recriei o HQL create_table_vendas.hql para refletir essa mudança.

Voltei e executei o HIVE para corrigir as tabelas que ficaram erradas.

Usei o beeline para dropar as tabelas.

Executei novamente o script carga_tabelas.sh.

Alterei e executei novamente o process.py com a ordem correta das colunas para a tabela VENDAS.

Voltei ao POWERBI e importei as tabelas: dim_vendas, dim_tempo e ft_vendas.

Verifiquei que os campos da tabela dim_tempo estavam vazios, revisei o código de process.py e estavam de acordo com as instruções da aula, então deixei assim mesmo, ou seja, no programa feito em python as conversões: year, month, dayofmonth, quarter; foram corretamente utilizadas. De qualquer forma na tabela dimensão TEMPO tem também o campo ds_invoice_date que servirá bem para responder as perguntas relativas à dimensão.

Reparei também que as colunas que criei para a tabela FATO vendas.csv também estavam nulos, verifiquei process.py novamente e me pareceu de acordo com as aulas.

No POWERBI, transformei as colunas da tabela dim_vendas:
id_costumer_key foi transformada para TEXTO;
id_item_class foi transformada para TEXTO;
ds_list_prince foi transformada para TEXTO;
ds_amount_list_prince foi transformado para DECIMAL;
ds_sales_cost_amount foi transformado para DECIMAL;
ds_sales_margin_amount foi transformado para DECIMAL;
ds_sales_price foi transformado para DECIMAL;
ds_sales_quantity foi transformado para DECIMAL;
id_sales_rep foi transformada para TEXTO;

As colunas id_order_number, ds_promised_delivery_date e ds_sales_amount também parecem ter os dados trocados. Não alterei porque não sei ao certo a ordem correta.



Pela tabela FATO ter sido montada com dados nulos, não foi possível construir o gráfico no POWERBI do jeito que era para ter ficado, ou seja, sumarizado corretamente pela tabela FATO.


No POWERBI não foi possível criar os gráficos: Vendas por cliente e Vendas por estado. Porque não há dados para cliente e nem estado. As tabelas fornecidas foram vendas, regiao, divisao e endereço. Não há como relacionar estas tabelas e conseguir vendas por clientes e nem vendas por estado. 

Mais importante não há relacionamentos entre as chaves das tabelas vendas, regiao, divisão e endereço. Assim só foi possível se criar a dimensão vendas e a dimensão tempo. Além lógico da tabela fato VENDAS. Como já explicado por um motivo que desconheço não consegui criar um campo para sumarizar o total de vendas e quantidade de vendas na tabela fato VENDAS, isto é, eu faço o comando para sumarizar mas as colunas ficaram com o valor NULO. Fiz tudo certo mas não sumarizou. Então nos gráficos do POWERBI não utilizei a tabela FATO e sim as tabelas de dimensão vendas e tempo.
