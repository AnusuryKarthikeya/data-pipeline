import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = SparkSession.builder.appName("tests").master("local[*]").getOrCreate()
    yield session
    session.stop()
