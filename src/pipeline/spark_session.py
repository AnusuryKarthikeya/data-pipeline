"""Shared SparkSession factory configured for Delta Lake."""
import os
import sys

# Windows only: point Spark at winutils/hadoop.dll before Spark starts.
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] = os.environ["PATH"] + os.pathsep + r"C:\hadoop\bin"

# Make Spark's Python workers use THIS interpreter, not a stray system 'python'.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark(app_name: str = "pipeline") -> SparkSession:
    """Return a SparkSession with Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()