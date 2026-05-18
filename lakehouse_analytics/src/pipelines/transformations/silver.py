from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

@dp.materialized_view(name="silver.sales_fact")
def silver_transform():

    df = (
        spark.read.table("bronze.fact_sales")
        .withColumn("data_venda",F.to_timestamp(F.col("data_venda"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "data_venda", F.to_date(F.col("data_venda")))
        .withColumn("pais", F.trim(F.split(F.col("localidade"), "-").getItem(0)))
        .withColumn("continente", F.trim(F.split(F.col("localidade"), "-").getItem(1)))
        .withColumn("sobrenome", F.trim(F.split(F.col("nome_cliente"), ",").getItem(0)))
        .withColumn("nome", F.trim(F.split(F.col("nome_cliente"), ",").getItem(1)) )
        .withColumn("nome_cliente", F.concat_ws(" ", F.col("nome"), F.col("sobrenome")))
        .withColumn("custo_unitario", F.col("custo_unitario").cast(DecimalType(10, 2)))
        .withColumn("preco_unitario", F.col("preco_unitario").cast(DecimalType(10, 2)))
        .withColumn("qtd_vendida", F.col("qtd_vendida").cast(IntegerType()))
        .drop("localidade", "nome", "sobrenome")
        .dropDuplicates()
    )

    # Padronizar nomes
    for old_col in df.columns:
        new_col = (
            old_col.lower()
                   .replace(" ", "_")
                   .replace(".", "")
        )

        df = df.withColumnRenamed(old_col, new_col)

    # Trim nas colunas string
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.trim(F.col(field.name))
            )

    columns_order = [
       "data_venda", 
       "produto",
       "categoria",
       "nome_cliente",
       "pais",
       "continente",
       "qtd_vendida",
       "custo_unitario",
       "preco_unitario",
       "updated_at"
    ]
    
    df = df.select(*columns_order)

    return df



