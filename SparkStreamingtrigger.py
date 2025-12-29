# trigger((processingTime= "30 Seconds") continuous stream job
# process data every fixed time interval, which is 30 sec here
# means, every 30sec, spark checks for new data & proceess it.
# keeps running until manually stoped
# continuous file ingestion process. 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import getpass
username = getpass.getuser()
print(username)

if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
            .builder \
            .appName("debu application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .config("spark.driver.bindAddress", 'localhost') \
            .config("spark.ui.port", "4050") \
            .config("spark.driver.port", "4051") \
            .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
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
    .format("console") \
    .outputMode("complete") \
    .option("checkpointLocation","checkpointdir108") \
    # spark check for new data every 30 seconds, if new data available, then it process, otherwise it'll checks again after 30 seconds.
    .trigger(processingTime= "30 Seconds") \
    .toTable("orders_final_result")

    spark.sql("create table orders_final_result (customer_id long, orders_placed long, products_purchased long, amount_spent float)")

    spark.sql("select * from orders_final_result").show()
