from pathlib import Path

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType


class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end


class ZipDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.numPartitions = int(self.options.get("numPartitions", 2))
        print(options)

    def partitions(self):
        return [RangePartition(0, 1000) for i in range(self.numPartitions)]

    def read(self, partition):
        # Library imports must be within the method.
        from zipfile import ZipFile

        print(partition)
        try:
            p = Path(self.path)
            if not p.exists():
                print(f"not exists {p}")
                return
            if p.is_dir():
                # a folder full of zips
                # beaware of .glob() at extreme
                for file in Path(self.path).glob("**/*.zip"):
                    print(f"{file}")
                    with ZipFile(file, "r") as zipFile:
                        for name in zipFile.namelist():
                            with zipFile.open(name, "r") as zipfile:
                                for line in zipfile:
                                    yield [f"{file}, {name}, {line.rstrip()}"]
            else:
                # single zip file
                with ZipFile(file, "r") as zipFile:
                    for name in zipFile.namelist():
                        with zipFile.open(name, "r") as zipfile:
                            for line in zipfile:
                                print(type(line))
                                yield [f"{file}, {name}, {line.rstrip()}"]
        except Exception as e:
            print(e)


from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType


class ZipDataSource(DataSource):
    """
    An example data source for batch query using the `Zipr` library.
    """

    @classmethod
    def name(cls):
        return "Zip"

    def schema(self):
        return "line string"

    def reader(self, schema: StructType):
        return ZipDataSourceReader(schema, self.options)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session(app_name="PySpark CSV Zips Datasource Tester"):
    """
    Creates and returns a Spark Session
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .getOrCreate()
    )


if __name__ == "__main__":
    print("Hello")

    spark = create_spark_session()
    spark.dataSource.register(ZipDataSource)
    # TODO: Fix re-create CSV data
    # df = spark.read.format("Zip").load("./zips/x.zip").limit(3)
    # print(df.collect())

    df = spark.read.format("Zip").load("./zips")
    df.write.format("text").mode("overwrite").save("./saves")
