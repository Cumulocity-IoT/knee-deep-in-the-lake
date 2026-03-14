import os
from pathlib import Path


def get_spark(app_name="IcebergTraining", gold_warehouse=None, silver_warehouse=None, log_level="WARN"):
    """
    Return a SparkSession configured with Iceberg Spark runtime.

    Downloads the Iceberg Spark runtime JAR automatically on first call
    via spark.jars.packages (requires internet access).

    Two Hadoop catalogs are registered:
    - 'local'  → gold_warehouse  (for writing gold tables)
    - 'silver' → silver_warehouse (optional, for reading PyIceberg-written silver tables)

    Use 'local.<namespace>.<table>' for gold tables in SQL/writeTo.
    Use 'silver.<namespace>.<table>' to read silver tables written by PyIceberg.

    The Hadoop catalog follows the same <warehouse>/<namespace>/<table> layout
    that PyIceberg's SqlCatalog uses, so both tools can access the same tables
    by pointing at the same warehouse directory.

    Args:
        app_name: Spark application name shown in the Spark UI.
        gold_warehouse: Local path for the gold Spark Hadoop catalog.
                        Defaults to ../data/warehouse_gold relative to this file.
        silver_warehouse: Optional local path for the silver PyIceberg warehouse.
                          When provided, tables are readable as 'silver.<ns>.<table>'.
        log_level: Spark log level (WARN, INFO, ERROR).

    Returns:
        A configured SparkSession.
    """
    from pyspark.sql import SparkSession

    if gold_warehouse is None:
        gold_warehouse = str(Path(__file__).parent.parent / "data" / "warehouse_gold")
    gold_warehouse = os.path.abspath(gold_warehouse)

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg Spark runtime: Spark 4.0/4.1 uses Scala 2.13.
        # No 4.1-specific runtime exists yet; the 4.0 runtime is fully compatible.
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
        )
        # Enough heap for 1.3M-row datasets loaded via Arrow→pandas→Spark.
        .config("spark.driver.memory", "3g")
        # Enable Iceberg SQL extensions (MERGE INTO, CALL procedures, etc.)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # 'local' catalog: Hadoop-backed, used for writing gold tables.
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"file://{gold_warehouse}")
    )

    if silver_warehouse is not None:
        silver_warehouse = os.path.abspath(silver_warehouse)
        # 'silver' catalog: Hadoop-backed, points at the PyIceberg-written warehouse.
        # PyIceberg SqlCatalog uses <warehouse>/<namespace>/<table> layout — same as
        # a Spark Hadoop catalog — so both tools can read the same table files.
        builder = (
            builder
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.silver.type", "hadoop")
            .config("spark.sql.catalog.silver.warehouse", f"file://{silver_warehouse}")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def inspect_gold_table(spark, table_name):
    """
    Print schema, snapshot count, row count, and a sample for a Spark Iceberg table.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified table name, e.g. 'local.gold.device_hourly'.
    """
    print(f"Table: {table_name}")
    print()

    df = spark.table(table_name)
    print("Schema:")
    df.printSchema()

    count = df.count()
    print(f"Row count: {count:,}")

    try:
        history = spark.sql(f"SELECT * FROM {table_name}.history ORDER BY made_current_at DESC").collect()
        print(f"Snapshots: {len(history)}")
        if history:
            latest = history[0]
            print(f"Latest snapshot: {latest['snapshot_id']} at {latest['made_current_at']}")
    except Exception:
        pass

    print("\nSample (5 rows):")
    df.show(5, truncate=False)
