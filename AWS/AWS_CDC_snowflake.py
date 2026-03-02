
####    winget install --id Amazon.Corretto.8.JDK --source winget


import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_482'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


#Snowfalke alone - Working fine
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.30 pyspark-shell'

#Snowfalke_s3
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.30,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.1026 pyspark-shell'


spark = SparkSession.builder \
    .appName("pyspark") \
    .master("local[*]") \
    .getOrCreate()



spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print("======== Started=========")

import sys
import traceback
import time
from datetime import datetime
from pyspark.sql.types import *

print(spark.version)

print(spark.sparkContext.getConf().get("spark.jars.packages"))
print('====Reading SnowFlake=====')
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")

SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WH = os.getenv("SNOWFLAKE_WH")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

print(SNOWFLAKE_ACCOUNT)

sdf = (
    spark.read.format("snowflake")
    .option("sfURL", SNOWFLAKE_ACCOUNT)
    .option("sfUser", SNOWFLAKE_USER)
    .option("sfPassword", SNOWFLAKE_PASSWORD)
    .option("sfDatabase", SNOWFLAKE_DB)
    .option("sfSchema", SNOWFLAKE_SCHEMA)
    .option("sfRole", SNOWFLAKE_ROLE)
    .option("sfWarehouse", SNOWFLAKE_WH)
    .option("dbtable", SNOWFLAKE_TABLE)
    .load()
)

sdf.show(5)
print('====Reading s3=====')

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")


spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

df_s3=spark.read.format('csv').option('header','True').load('s3a://s3-first-bucket-10012026/employees.csv')
df_s3.show(1)

spark.stop()
