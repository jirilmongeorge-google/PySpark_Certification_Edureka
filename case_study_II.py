# Case Study: Log ParsingDomain: Telecom

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

raw = spark.read.text("use_cases/access.clean.log")

from pyspark.sql.types import Row
df=raw.rdd.map(lambda r:r[0].split("")).map(lambda arr:Row(remote_host=arr[0],timestamp=arr[3].replace("[",""),request_type=arr[5],url=arr[6],status_code=arr[8])).toDF()

# Find out how many 404 HTTP codes are in access logs.
df.filter(df.status_code == '404').count()

# Replace null values with constants such as 0
df2 = df.na.fill("404",["status_code"])

# Parse timestamp to readable date.
from pyspark.sql.functions as F
df3 = df2.withColumn("Date", F.to_date(   F.unix_timestamp(df2."timestamp", "dd/MM/yyyy").cast("timestamp")   ))

# Describe which HTTP status values appear in data and how many.
df3.groupBy("status_code").count().sort(F.col("count").desc()).show()

# Find out How many unique hosts are there in entire log and their average request
df3.select("remote_host").distinct().count()





























