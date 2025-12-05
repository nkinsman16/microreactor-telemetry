from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max as max_, min as min_, count

spark = (
    SparkSession.builder
    .appName("ReactorGoldMetrics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bronze_path = "output/reactor_delta"
gold_path   = "output/reactor_metrics_1min"

df = spark.read.format("delta").load(bronze_path)

# If timestamp is a string, cast it
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

gold = (
    df
    .groupBy(
        "reactor_id",
        window(col("timestamp"), "1 minute").alias("w")
    )
    .agg(
        avg("PE").alias("avg_power_mw"),
        avg("T").alias("avg_temp"),
        avg("AP").alias("avg_pressure"),
        avg("RH").alias("avg_humidity"),
        max_("PE").alias("max_power_mw"),
        min_("PE").alias("min_power_mw"),
        count("*").alias("num_readings")
    )
    .selectExpr(
        "reactor_id",
        "w.start as window_start",
        "w.end as window_end",
        "avg_power_mw",
        "avg_temp",
        "avg_pressure",
        "avg_humidity",
        "max_power_mw",
        "min_power_mw",
        "num_readings"
    )
)

(
    gold.write
        .format("delta")
        .mode("overwrite")
        .save(gold_path)
)

print(f"Wrote gold metrics table to: {gold_path}")
spark.stop()
