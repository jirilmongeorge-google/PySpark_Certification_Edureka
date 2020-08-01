# Case Study: InstacartDomain: E-commerce

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql.functions as F

# Load datasets
aisles = spark.read.csv("hdfs://loalhost:9000/user/edureka_749763/aisles.csv").toDF("aisle_id","aisle")
departments = spark.read.csv("hdfs://loalhost:9000/user/edureka_749763/departments.csv").toDF("department_id","department")
products = spark.read.csv("hdfs://loalhost:9000/user/edureka_749763/products.csv").toDF("product_id","product_name","aisle_id","department_id")
orders = spark.read.csv("hdfs://loalhost:9000/user/edureka_749763/orders.csv").toDF("order_id","user_id","evala_set","order_number","order_dow","order_hour_of_day","days_since_prior_order")
orders_train = spark.read.csv("hdfs://loalhost:9000/user/edureka_749763/order_products__train.csv").toDF("order_id","product_id","add_to_cart_order","reordered")

# Merge all the data frames based on the common key and create a single DataFrame 
df = products.join(aisles, aisles.aisle_id == products.aisle_id, "left").drop(aisles.aisle_id)
.join(departments, departments.department_id == products.department_id, "left").drop(departments.department_id)
.join(orders_train, orders_train.product_id == products.product_id, "right").drop(orders_train.product_id)
.join(orders, orders.order_id == orders_train.order_id, "left").drop(orders.order_id)

# Check missing dataGeneratefilteredconditiondynamically
filtered = df.filter("order_id is not null")

# List the most ordered products (top 10)
filtered.groupBy("product_name").count().sort(F.col("count").desc()).show(10)

# Do people usually reorder the same previous ordered products
filtered.groupBy("product_name").agg(F.avg("reordered"), F.count("reordered")).sort(col("count(reordered)").desc()).show(10)

# Most important department and aisle (by number of products)
filtered.groupBy("department_id", "aisle_id").agg(F.count(filtered.product_id)).sort(col("count(product_id)").desc()).show(10)
filtered.groupBy("aisle_id").agg(F.count(filtered.product_id)).sort(col("count(product_id)").desc()).show(10)

# Get the Top 10 departments
filtered.groupBy("department").count().sort(col("count").desc()).show(10)

# List top 10 products ordered in the morning (6 AM to 11 AM)
filtered.filter("order_hour_of_day >= 6 and order_hour_of_day <= 11")
.groupBy("product_name")
.count()
.sort(col("count").desc())
.show(10)









































