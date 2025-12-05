from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, to_timestamp, to_date, window,
    avg, max as spark_max, min as spark_min, count
)

RAW_PATH = "output/reactor_readings"
DELTA_PATH = "delta/reactor_readings"


def create_spark() -> SparkSession:
    """
    Create a Spark session with Delta support.
    (spark-submit already passes the Delta jars, so we just need the extensions here.)
    """
    spark = (
        SparkSession.builder
        .appName("MicroreactorTelemetryDeltaAnalysisV2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_schema() -> StructType:
    """
    Explicit schema for reactor telemetry.
    Adjust fields as your simulator grows.
    """
    return StructType([
        StructField("reactor_id", StringType(), True),

        StructField("power_mw", DoubleType(), True),
        StructField("coolant_temp_c", DoubleType(), True),
        StructField("coolant_pressure_mpa", DoubleType(), True),
        StructField("coolant_flow_kg_s", DoubleType(), True),

        StructField("neutron_flux_nv", DoubleType(), True),
        StructField("control_rod_position_pct", DoubleType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("core_inlet_temp_c", DoubleType(), True),
        StructField("core_outlet_temp_c", DoubleType(), True),

        # event_time as an ISO8601 string in the JSON
        StructField("event_time", StringType(), True),
    ])


def load_raw_json(spark: SparkSession):
    """
    Load raw JSON from the output folder with the explicit schema.
    """
    print(f"=== Loading JSON from {RAW_PATH} ===")

    schema = get_schema()

    df = (
        spark.read
        .schema(schema)
        .json(RAW_PATH)
    )

    # Normalize event_time to TimestampType; if missing, use current_timestamp
    df = df.withColumn(
        "event_ts",
        to_timestamp(col("event_time"))
    )

    # For partitioning and grouping
    df = df.withColumn("event_date", to_date(col("event_ts")))

    print("=== Raw JSON schema ===")
    df.printSchema()

    print("=== Sample JSON rows ===")
    df.show(20, truncate=False)

    return df


def write_to_delta(df):
    """
    Write to Delta Lake, partitioned by reactor_id and event_date.
    """
    print(f"=== Writing data to Delta table at: {DELTA_PATH} ===")

    (
        df.write
        .format("delta")
        .mode("append")  # change to "overwrite" if you want a clean table each run
        .partitionBy("reactor_id", "event_date")
        .save(DELTA_PATH)
    )


def read_from_delta(spark: SparkSession):
    """
    Read back from Delta Lake and show a sample.
    """
    print(f"=== Reading from Delta table at: {DELTA_PATH} ===")

    df = (
        spark.read
        .format("delta")
        .load(DELTA_PATH)
    )

    print("=== Delta schema ===")
    df.printSchema()

    print("=== Sample Delta rows ===")
    df.show(20, truncate=False)

    return df


def compute_batch_aggregates(df):
    """
    Compute simple batch aggregates:
      - average / min / max coolant temperature per reactor
      - count of events per reactor
    """
    print("=== Per-reactor aggregate stats (batch) ===")

    agg = (
        df.groupBy("reactor_id")
        .agg(
            count("*").alias("event_count"),
            avg("coolant_temp_c").alias("avg_coolant_temp_c"),
            spark_min("coolant_temp_c").alias("min_coolant_temp_c"),
            spark_max("coolant_temp_c").alias("max_coolant_temp_c"),
            avg("power_mw").alias("avg_power_mw"),
        )
        .orderBy("reactor_id")
    )

    agg.show(100, truncate=False)


def compute_time_window_aggregates(df):
    """
    Compute time-window aggregates:
      - 5-minute rolling windows of average temp and power per reactor.
    This is batch-style; we’ll mirror this logic in streaming later.
    """
    print("=== 5-minute window aggregates (batch-style) ===")

    # Only keep rows where event_ts is not null
    df = df.where(col("event_ts").isNotNull())

    win = (
        df.groupBy(
            "reactor_id",
            window(col("event_ts"), "5 minutes")  # you can change to "10 seconds" for tests
        )
        .agg(
            avg("coolant_temp_c").alias("avg_coolant_temp_c"),
            avg("power_mw").alias("avg_power_mw"),
            count("*").alias("event_count")
        )
        .orderBy("reactor_id", "window")
    )

    win.show(100, truncate=False)


def main():
    spark = create_spark()

    # 1. Load raw JSON
    raw_df = load_raw_json(spark)

    # 2. Write it to Delta, partitioned
    write_to_delta(raw_df)

    # 3. Read back from Delta
    delta_df = read_from_delta(spark)

    # 4. Run aggregate analyses
    compute_batch_aggregates(delta_df)
    compute_time_window_aggregates(delta_df)

    spark.stop()


if __name__ == "__main__":
    main()
