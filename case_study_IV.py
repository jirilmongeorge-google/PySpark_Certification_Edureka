# Case Study: Mobile App StoreDomain: Telecom

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql.functions as F 

raw = spark.read.text("use_cases/access.clean.log")
df = raw.rdd.map(lambda r:r[0].split(" "))
.map(lambda arr: Row(remote_host=arr[0], timestamp=arr[3].replace("[",""), request_type=arr[5],url=arr[6],status_code=arr[8]))
.toDF()

df.registerTempTable("logs")

# Find out how many 404 HTTP codes are in access logs
spark.sql("select count(*) from logs where status_code == '404'").show()

# Find out which URLs are broken
spark.sql("select * from logs where status_code == '204'").show()

# Verify there are no null columns in the original dataset
condition = "is null or ".join(df.columns) + " is null"
spark.sql("select count(*) from logs where " + condition).show()

# Replace null values with constants such as 0
df1 = spark.sql("select remote_host, timestamp, request_type, url, nvl(cast(status_code as String),'404') as status_code from logs”)

# Parse timestamp to readable date
spark.sql("select remote_host, request_type, status_code, to_date(cast(unix_timestamp(timestamp, 'dd/MMM/yyyy') as timestamp)) as Date from logs").show()
                
# Describe which HTTP status values appear in data and how many
spark.sql("select status_code, count(*) as code_count from logs group by status_code order by code_count desc").show() 
                

                