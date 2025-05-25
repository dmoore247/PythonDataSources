import logging
import sys

import pytest
from pyspark.sql import SparkSession

from zip_dcm_ds import ZipDCMDataSource, ZipDCMDataSourceReader

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.appName("PySpark DCM Zips Datasource Tester")
        .master("local[*]")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .getOrCreate()
    )
    spark_session.dataSource.register(ZipDCMDataSource)
    yield spark_session
    spark_session.stop()


def test_ZipDCMDataSourceReader():
    zip_file_path = "./resources/dcms"
    r = ZipDCMDataSourceReader(
        schema="rowid INT, path STRING, meta STRING",
        options={"path": zip_file_path, "numPartitions": 32},
    )
    partitions = r.partitions()
    logger.debug([_ for _ in partitions])

    for part in partitions:
        results = r.read(part)
        logger.debug([_ for _ in results])

def test_wrongfile(spark):
    from pyspark.errors import AnalysisException
    with pytest.raises(AnalysisException):
        df = (
            spark.read.option("numPartitions", "1")
            .format("zipdcm")
            .load("./resources/wrongpath.zip")
        )
        result = df.collect()

def test_wrongpath(spark):
    from pyspark.errors import AnalysisException
    with pytest.raises(AnalysisException):
        df = (
            spark.read.option("numPartitions", "1")
            .format("zipdcm")
            .load("./resources/wrongpath")
        )
        result = df.collect()

def test_dcm(spark):
    df = (
        spark.read.option("numPartitions", "1")
        .format("zipdcm")
        .load("./resources/dcms/y/1-1.dcm")
    )
    result = df.collect()
    assert len(result) == 1
    logger.debug(f"test_single result: {result}")

def test_single(spark):
    df = (
        spark.read.option("numPartitions", "1")
        .format("zipdcm")
        .load("./resources/dcms/3.5.574.1.3.9030958.6.376.2860280475000825621.zip")
    )
    result = df.collect()
    assert len(result) == 1
    logger.debug(f"test_single result: {result}")


def test_folder(spark, tmp_path):
    df = (
        spark.read.option("numPartitions", "4")
        .format("zipdcm")
        .load("./resources/dcms")
    )
    df.limit(20).show()
    save_path = tmp_path / "saves"
    df.write.format("csv").mode("overwrite").save(str(save_path))
    assert save_path.exists()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main([__file__]))
