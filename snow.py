from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("FirstServerLessJob").getOrCreate()

sdf = (
    spark.read.format("snowflake")
    .option("sfURL", "https://zklaofq-ub28057.snowflakecomputing.com")
    .option("sfAccount", "zklaofq")
    .option("sfUser", "GOMBHUVANA")
    .option("sfPassword", "Liyansh@usa908")
    .option("sfDatabase", "zeyodb")
    .option("sfSchema", "zeyoschema")
    .option("sfRole", "ACCOUNTADMIN")
    .option("sfWarehouse", "COMPUTE_WH")
    .option("dbtable", "srctab")
    .load()
)
sdf.show(5)



aggdf = sdf.groupby("username").agg(collect_list("score").alias("scores"))

aggdf.write.format('parquet').mode('overwrite').save('s3://batch479-2026/dest/SnowOutput')