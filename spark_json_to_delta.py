from pyspark.sql import SparkSession

# --- Spark + Delta session ---
spark = (
    SparkSession.builder
    .appName("JsonToDeltaReactor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Paths relative to project root
json_path = "output/reactor_json"
delta_path = "output/reactor_delta"

print(f"Reading JSON from: {json_path}")
df = spark.read.json(json_path)

print("Schema:")
df.printSchema()

# Optional: reorder/select columns if needed
# Adjust these to match your Flink schema
expected_cols = [
    "reactor_id",
    "timestamp",
    "T",          # Temperature
    "V",          # Exhaust Vacuum
    "AP",         # Ambient Pressure
    "RH",         # Relative Humidity
    "PE",         # Electrical Energy Output
    "is_valid",   # example validation flag from Flink, if you have one
    "error_msg"   # optional
]

existing_cols = [c for c in expected_cols if c in df.columns]
df = df.select(*existing_cols)

# Write as Delta
(
    df.write
      .format("delta")
      .mode("overwrite")  # for now; can change to "append"
      .save(delta_path)
)

print(f"Wrote Delta table to: {delta_path}")

spark.stop()
