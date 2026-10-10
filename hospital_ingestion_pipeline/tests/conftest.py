import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# creating testing environment
@pytest.fixture(scope="session")
def spark():
    """Local Spark session for the entire test session."""

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Databricks uses existing version
        yield SparkSession.builder.getOrCreate()
        return
    
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("hospital-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session           # because we want to come back and stop session
    session.stop()
