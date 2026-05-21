import os
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from flights.transforms import flight_transforms, shared_transforms
from flights.utils import flight_utils

catalog = "main"
database = "flights_dev"

path = "/databricks-datasets/airlines/part-00000"
raw_table_name = f"{catalog}.{database}.flights_raw"

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()


def is_running_on_databricks():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None:
        return True
    else:
        return False

running_on_cluster = is_running_on_databricks()
print("Code running on Databricks (as worfklow or from workspace)?:", running_on_cluster)

df = flight_utils.read_batch(spark, path).limit(40)

df_transformed = (
        df.transform(flight_transforms.delay_type_transform)
          .transform(shared_transforms.add_metadata_columns)
    )

df_transformed.show()


# if running_on_cluster != True:
#     spark.addArtifact("src/flights/utils/flight_utils.py", pyfile=True)
# else:
#     spark.addArtifact("flights/utils/flight_utils.py", pyfile=True)

# @udf(returnType=StringType(), useArrow=True) 
# def clean_time_udf(str):
#     from flights.utils.flight_utils import clean_time_str
#     return clean_time_str(str)

# df_transformed2 = df.withColumn("CleanArrDelay", clean_time_udf(col("ArrDelay")))
# df_transformed2.select("CleanArrDelay").where("ArrDelay == 'NA'").show()

# # Read carrier info
# carrier_path = "/databricks-datasets/airlines/carriers.csv"
# carrier_df = spark.read.format("csv").option("header", "true").load(carrier_path)

# # Join with transformed data
# df_with_carrier = df_transformed.join(
#     carrier_df,
#     df_transformed.UniqueCarrier == carrier_df.Code,
#     "left"
# )

# # Calculate summary metrics
# summary_metrics = df_with_carrier.groupBy("Description").agg(
#     count("*").alias("total_flights"),
#     sum(when(col("delay_type").isNotNull(), 1).otherwise(0)).alias("delayed_flights"),
#     round(avg("ArrDelay"), 2).alias("avg_arrival_delay")
# ).orderBy("total_flights", ascending=False)

# # Show results
# print("\nFlight Summary by Carrier:")
# summary_metrics.show(truncate=False)


# # print(f"Reading data from {path}")
# df_transformed.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(raw_table_name)
# # print(f"Succesfully wrote data to {raw_table_name}")