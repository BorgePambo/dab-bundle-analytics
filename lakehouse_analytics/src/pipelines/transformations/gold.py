from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

@dp.expect("gold_daily_sales", "total_amount > 0")
@dp.materialized_view(name="gold.fact_sales_daily")
def gold_daily_sales():

    df = spark.read.table("silver.sales_fact")

    df = (
        df
        .withColumn("total_amount",
                    F.round(F.col("preco_unitario") * F.col("qtd_vendida"), 2))
        .withColumn("custo_total",
                    F.round(F.col("custo_unitario") * F.col("qtd_vendida"), 2))
        .withColumn("lucro",
                    F.round(F.col("total_amount") - F.col("custo_total"), 2))
        .withColumn("ano", F.year("data_venda"))
        .withColumn("mes_num", F.month("data_venda"))
        .withColumn("dia", F.dayofmonth("data_venda"))
        .withColumn(
            "mes",
            F.when(F.col("mes_num") == 1, "Janeiro")
             .when(F.col("mes_num") == 2, "Fevereiro")
             .when(F.col("mes_num") == 3, "Março")
             .when(F.col("mes_num") == 4, "Abril")
             .when(F.col("mes_num") == 5, "Maio")
             .when(F.col("mes_num") == 6, "Junho")
             .when(F.col("mes_num") == 7, "Julho")
             .when(F.col("mes_num") == 8, "Agosto")
             .when(F.col("mes_num") == 9, "Setembro")
             .when(F.col("mes_num") == 10, "Outubro")
             .when(F.col("mes_num") == 11, "Novembro")
             .when(F.col("mes_num") == 12, "Dezembro")
        )
    )

    df = (
        df.groupBy(
            "ano", "mes" , "dia", "continente", "pais",
            "produto", "categoria", "nome_cliente"
        )
        .agg(
            F.sum("qtd_vendida").alias("qtd_vendida"),
            F.sum("total_amount").alias("total_amount"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.sum("custo_total").alias("custo_total"),
            F.round(F.avg("custo_total"), 2).alias("avg_custo_total"),
            F.sum("lucro").alias("lucro")
        )
    )

    return df



@dp.materialized_view(name="gold.sales_by_region")
def gold_sales_by_continent_country():

    df = spark.read.table("silver.sales_fact")

    df = df.withColumn(
        "total_amount",
        F.round(F.col("preco_unitario") * F.col("qtd_vendida"), 2)
    )

    df = (
        df.groupBy("continente", "pais")
        .agg(
            F.sum("total_amount").alias("total_amount"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount")
        )
    )

    return df




