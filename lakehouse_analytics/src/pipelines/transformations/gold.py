from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="gold.fact_sales_daily")
def gold_daily_sales():

    df = spark.read.table("silver.sales_fact")

    df = (
        df
        .withColumn("total_amount", F.col("preco_unitario") * F.col("qtd_vendida"))
        .withColumn("custo_total", F.col("custo_unitario") * F.col("qtd_vendida"))
        .withColumn("lucro", F.col("total_amount") - F.col("custo_total"))
        .withColumn("ano", F.year("data_venda"))
        .withColumn("mes_num", F.month("data_venda"))
        .withColumn("dia", F.day("data_venda"))
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
            "ano", "mes", "continente", "pais",
            "produto", "categoria", "nome_cliente", "dia"
        )
        .agg(
            F.sum("total_amount").alias("total_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("custo_total").alias("custo_total"),
            F.avg("custo_total").alias("avg_custo_total"),
            F.sum("lucro").alias("lucro")
        )
    )

    return df