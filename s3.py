from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FirstSeverlessJob").getOrCreate()

df = spark.read.load("s3://batch479-2026/src")

df.show()
df.write.format('parquet').mode('overwrite').save('s3://batch479-2026/dest/S3Output')