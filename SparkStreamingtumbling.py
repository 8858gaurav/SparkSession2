# put the line by line from Dataset to this i/p script console.

from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[2]")
my_conf.set("spark.sql.shuffle.partitions", 3)

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orders_schema = "order_id long, order_date timestamp, order_customer_id long, order_status string, amount long"

# creating the dataframe
# in another terminal type nc -lk 9971
orders_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9971") \
    .load()

value_df = orders_df.select(from_json(col("value"), orders_schema).alias("Value"))

value_df.printSchema()

refined_orders_df = value_df.select("value.*")

refined_orders_df.printSchema()

window_agg_df = refined_orders_df \
.groupBy(window(col("order_date"), "15 minutes")) \
.agg(sum("amount").alias("total_invoice"))

window_agg_df.printSchema()

output_df = window_agg_df.select("window.start", "window.end", "total_invoice")

#append mode will not work for aggregations.
# appened output mode not supported when there are streaming aggregations on streaming dataframe/Datasets without watermark.
query = output_df \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "checkpointlocation11") \
    .trigger(processingTime= "15 second") \
    .start()

query.awaitTermination()