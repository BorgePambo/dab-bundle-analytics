from pyspark import pipelines as dp
from pyspark.sql import functions as F

folder = "/Volumes/ecommerce_dev/raw/dataset/ecommerce/vendas/"

@dp.table(name="bronze.fact_sales")
def source_data_sales():

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(folder)
        .withColumn("updated_at", F.current_timestamp())
    )

    df = (
        df.withColumnRenamed("Data da Venda", "data_venda")
          .withColumnRenamed("Custo Unitário", "custo_unitario")
          .withColumnRenamed("Qtd. Vendida", "qtd_vendida")
          .withColumnRenamed("Nome Cliente", "nome_cliente")
          .withColumnRenamed("PrecoUnitario", "preco_unitario")
          .withColumnRenamed("Localidade", "localidade")
    )

    return df