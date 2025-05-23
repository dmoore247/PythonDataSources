from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from zip_dcm_ds import ZipDCMDataSource

import logging
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s')
# Create a handler (e.g., StreamHandler for console output)
handler = logging.StreamHandler()
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


def create_spark_session(app_name="PySpark DCM Zips Datasource Tester"):
    """
    Creates and returns a Spark Session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "1g") \
        .getOrCreate()

def test_single(spark):
    # test a single zip (with a dcm and a license file)
    df = (
        spark.read
            .option('numPartitions','1')
            .format("zipdcm")
            .load("./resources/dcms/3.5.574.1.3.9030958.6.376.2860280475000825621.zip")
    )
    result = df.collect()
    assert len(result) == 1
    print(result)
    
def test_folder(spark):
    # test on a folder of zips
    df = (
        spark.read
          .option('numPartitions','4')
          .format("zipdcm")
          .load("./resources/dcms")
    )
    df.limit(20).show()
    df.write.format('csv').mode('overwrite').save('./saves')

if __name__ == "__main__":
    logger.debug('Started')
    
    spark = create_spark_session()
    
    # Add our custom Python DataSource for DICMO files storaged in a Zip Archive
    spark.dataSource.register(ZipDCMDataSource)

    test_single(spark)
    test_folder(spark)
    logger.debug('Finished')