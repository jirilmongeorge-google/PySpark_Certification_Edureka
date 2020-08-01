from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

raw = spark.read.text("use_cases/Appstore/input.csv")
print(raw)

from pyspark.sql.types import Row
df=raw.rdd.map(lambda r:r[0].split(",")).map(lambda arr:Row(id=arr[1],app_name=arr[2],size_bytes=long(arr[3]),rating_count_tot=long(arr[6]),prime_genre=arr[12],screenshots=int(arr[14]),langnum=int(arr[15]))).toDF()

from pyspark.sql.functions import col
df1 = df.withColumn("size_mb", size_bytes/1000000).withColumn("size_gb", col("size_mb")/1000)

df1.sort(df1.rating_count_tot.desc()).show(10)

#CalculateDifference in the average number of screenshots displayed of highest and lowest rating apps.
from pyspark.sql.functions as F
min_rating,max_rating = df1.agg(F.min(df1.rating_count_tot), F.max(df1.rating_count_tot)).collect()[0]
df1.filter(df1.rating_count_tot == min_rating).agg(F.avg(df1.screenshots)).show()
df1.filter(df1.rating_count_tot == max_rating).agg(F.avg(df1.screenshots)).show()

#Calculate what percentage of high rated apps support multiplelanguages.
df1.filter(df1.langnum > 1).count()*100 / df1.count()

#Find out How does app details contribute to user ratings. For this we will be using rating percentiles and then will be checking the averages ofdetails.
percentiles=df1.stat.approxQuantile("rating_count_tot",[0.25,0.50,0.75],0.0)
df_25=df1.filter(df1.rating_count_tot<percentiles[0])
df_50=df1.filter(df1.rating_count_tot<percentiles[1])
df_75=df1.filter(df1.rating_count_tot<percentiles[2])
df_100=df1.filter(df1.rating_count_tot > percentiles[2])
df_25.agg(F.avg(df_25.langnum)).show()
df_50.agg(F.avg(df_50.langnum)).show()
df_75.agg(F.avg(df_75.langnum)).show()
df_100.agg(F.avg(df_100.langnum)).show()

# Compare the statistics of different app groups/genres.
df1.groupBy("prime_genre").agg(F.avg(df1.langnum),F.avg(df1.screenshots),F.avg(df1.rating_count_tot),F.avg(df1.size_mb)).show()
















