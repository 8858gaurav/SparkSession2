# trigger batch stream job
# automativaly stops when there is no more data available.
# process all data that is currently available
# run finite micro-batches
# cluster stops automatically when there is no data available. auto terminate the cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import getpass
username = getpass.getuser()
print(username)
spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .config("spark.driver.bindAddress", 'localhost') \
    .config("spark.ui.port", "4050") \
    .config("spark.driver.port", "4051") \
    .enableHiveSupport() \
    .master("local[2]") \
    .getOrCreate()

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

orders_df = spark \
.readStream \
.format("json") \
.schema(orders_schema) \
.option("path","/Users/gauravmishra/Desktop/BoundedStreaming/Dataset2") \
.load()

orders_df.createOrReplaceTempView("orders")

exploded_orders = spark.sql("""select order_id,customer_id,city,state,
pincode,explode(line_items) lines from orders""")

exploded_orders.createOrReplaceTempView("exploded_orders")

flattened_orders = spark.sql("""select order_id, customer_id, city, state, pincode, 
lines.order_item_id as item_id, lines.order_item_product_id as product_id,
lines.order_item_quantity as quantity,lines.order_item_product_price as price,
lines.order_item_subtotal as subtotal from exploded_orders""")

flattened_orders.createOrReplaceTempView("orders_flattened")

aggregated_orders = spark.sql("""select customer_id, count(distinct order_id) as orders_placed, 
count(item_id) as products_purchased,sum(subtotal) as amount_spent 
from orders_flattened group by customer_id""")

spark.sql("drop table orders_final_result")

# update mode will work with delta table only.
streaming_query = aggregated_orders \
.writeStream \
.format("csv") \
.outputMode("complete") \
.option("checkpointLocation","checkpointdir108") \
# spark save progress (metadata) under checkpointdir108
# .trigger(), tells sparks when to execute streaming processing & how often to create a microbatches.
# process all currently available data, run multiple bicro batches if needed.
.trigger(availableNow= True) \
.toTable("orders_final_result")

spark.sql("create table orders_final_result (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

spark.sql("select * from orders_final_result").show()
