from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FirstServerLessJob").getOrCreate()

s3data = spark.read.load("s3://batch479/dest/s3output")
snowdata = spark.read.load("s3://batch479/dest/snowOutput")

joindf = s3data.join(snowdata, on=["username"], how="inner")

joindf.write.format("parquet").mode("overwrite").save("s3://batch479/dest/master")