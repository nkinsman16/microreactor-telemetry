from pyspark.sql import SparkSession

DELTA_PATH = "delta/reactor_telemetry"  # <- matches what you just listed


def main():
    spark = (
        SparkSession.builder
        .appName("MicroreactorTelemetryAnalysis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(DELTA_PATH)

    print("=== Delta table schema ===")
    df.printSchema()

    print("=== Sample rows ===")
    df.show(20, truncate=False)

    print("=== Count by reactor_id ===")
    df.groupBy("reactor_id").count().show()

    spark.stop()


if __name__ == "__main__":
    main()
