import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[2]").appName("fraud-tests")
         .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
