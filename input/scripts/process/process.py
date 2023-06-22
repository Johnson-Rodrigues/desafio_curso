
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f

import os
import re

spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[2]:


# tbl_vendas
df_vendas = spark.sql("select * from desafio_curso.tbl_vendas")

# tbl_divisao
df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")

# tbl_endereco
df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")

# tbl_regiao
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")


# In[3]:


df_divisao.show()


# In[4]:


df_vendas.show()


# In[5]:


df_regiao.show()


# In[6]:


df_endereco.show()


# In[7]:


df_vendas.printSchema()


# In[8]:


df_divisao.printSchema()


# In[9]:


df_endereco.printSchema()


# In[10]:


df_regiao.printSchema()


# In[11]:


df_vendas.createOrReplaceTempView('vendas')
df_divisao.createOrReplaceTempView('divisao')
df_endereco.createOrReplaceTempView('endereco')
df_regiao.createOrReplaceTempView('regiao')


# In[12]:


sql = '''
    select
    *
    from vendas 
    '''


# In[13]:


df_stage = spark.sql(sql)


# In[14]:


df_stage.show(5)


# In[15]:


# Criando dimensão tempo
df_stage = (df_stage
            .withColumn('Ano', year(df_stage.ds_invoice_date))
            .withColumn('Mes', month(df_stage.ds_invoice_date))
            .withColumn('Dia', dayofmonth(df_stage.ds_invoice_date))
            .withColumn('Trimestre', quarter(df_stage.ds_invoice_date))
                      )


# In[16]:


#


# In[17]:


df_stage.show(1)


# In[18]:


df_stage = df_stage.withColumn("DW_VENDAS", sha2(concat_ws("",
                                    df_stage.ds_actual_delivery_date,
                                    df_stage.id_costumer_key,
                                    df_stage.ds_invoice_date,
                                    df_stage.ds_discount_amount,
                                    df_stage.id_invoice_number,
                                    df_stage.id_item_class,
                                    df_stage.id_item_number,
                                    df_stage.ds_item,
                                    df_stage.ds_line_number,
                                    df_stage.ds_list_prince,
                                    df_stage.id_order_number,
                                    df_stage.ds_promised_delivery_date,
                                    df_stage.ds_sales_amount,
                                    df_stage.ds_sales_amount_list_prince,
                                    df_stage.ds_sales_cost_amount,
                                    df_stage.ds_sales_margin_amount,
                                    df_stage.ds_sales_price,
                                    df_stage.ds_sales_quantity,
                                    df_stage.id_sales_rep,
                                    df_stage.ds_um,
                                    df_stage.ds_dado1,
                                    df_stage.ds_dado2,
                                    df_stage.ds_dado3,
                                    df_stage.ds_dado4,
                                    df_stage.ds_dado5,
                                    df_stage.ds_dado6),256))  
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("",
                                    df_stage.ds_invoice_date,
                                    df_stage.Ano,
                                    df_stage.Mes,
                                    df_stage.Dia,
                                    df_stage.Trimestre),256))                  
                                                          


# In[19]:


df_stage.createOrReplaceTempView('stage')


# In[20]:


#Criando a tabela dimensao Vendas
dim_vendas = spark.sql('''
    SELECT DISTINCT
        DW_VENDAS,
        ds_actual_delivery_date,
        id_costumer_key,
        ds_invoice_date,
        ds_discount_amount,
        id_invoice_number,
        id_item_class,
        id_item_number,
        ds_item,
        ds_line_number,
        ds_list_prince,
        id_order_number,
        ds_promised_delivery_date,
        ds_sales_amount,
        ds_sales_amount_list_prince,
        ds_sales_cost_amount,
        ds_sales_margin_amount,
        ds_sales_price,
        ds_sales_quantity,
        id_sales_rep,
        ds_um,
        ds_dado1,
        ds_dado2,
        ds_dado3,
        ds_dado4,
        ds_dado5,
        ds_dado6
    FROM stage
        ''')


# In[21]:


dim_vendas.count()


# In[22]:


#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        ds_invoice_date,
        Ano,
        Mes,
        Dia,
        Trimestre
    FROM stage
''')


# In[23]:


dim_tempo.count()


# In[24]:


#Criando a tabela FATO Vendas
ft_vendas = spark.sql('''
    SELECT
        DW_VENDAS,
        DW_TEMPO,
        sum (ds_sales_price) as vl_de_venda,
        sum(ds_sales_quantity) as qtd_vendida
    FROM stage
    group by
        DW_VENDAS,
        DW_TEMPO
''')


# In[26]:


#


# In[27]:


ft_vendas.count()


# In[28]:


ft_vendas.show(5)


# In[29]:


# função para salvar os dados
def salvar_df(df, file):
    output = "/input/dasafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/" + file + "/part-* /input/desafio_curso/gold/" + file + ".csv"
    print (rename)
    
    df.coalesce(1).write        .format("csv")        .option("header", True)        .option("delimiter", ";")        .mode("overwrite")        .save("/datalake/gold/" + file + "/")
    os.system(erase)
    os.system(rename)
    
        


# In[30]:


salvar_df(dim_vendas,'dim_vendas')
salvar_df(dim_tempo,'dim_tempo')
salvar_df(ft_vendas,'ft_vendas')

