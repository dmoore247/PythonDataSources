from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType
from pyspark.sql.datasource import InputPartition


class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end


class FakeDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        print(f"options: {options}")

    def partitions(self):
        length = int(self.options.get("length", 0))
        partitions = int(self.options.get("partitions", 0))
        return [
            RangePartition(
                i * int(length / partitions), (i + 1) * int(length / partitions)
            )
            for i in range(partitions)
        ]

    def read(self, partition):
        # Library imports must be within the method.
        from faker import Faker
        import random
        import datetime

        fake = Faker()

        # Every value in this `self.options` dictionary is a string.
        # for client in clients:
        card_operations = []
        print(f"{partition.start} {partition.end}")
        for rowid in range(int(partition.start), int(partition.end)):
            transaction_id = fake.unique.uuid4()
            start_date = datetime.datetime.strptime("2000-01-01", "%Y-%m-%d")
            client_id = random.randint(1, 2000)
            tran_date = fake.date_between(
                start_date=start_date, end_date=datetime.timedelta(days=1)
            )
            operation = (
                str(transaction_id),
                str(client_id),
                str(fake.random_int(min=1, max=100000) / 10),
                tran_date.strftime("%Y-%m-%d"),
                fake.company(),
                random.choice(
                    [
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "approved",
                        "declined",
                    ]
                ),
            )
            yield operation


from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType


class FakeDataSource(DataSource):
    """
    An example data source for batch query using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "fake"

    def schema(self):
        return "transaction_id string, card_id string, transaction_amount string, transaction_date string, merchant string, status string"

    def reader(self, schema: StructType):
        return FakeDataSourceReader(schema, self.options)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session(app_name="PySpark Example Application"):
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
    print("Fake data generator")
    spark = create_spark_session()
    spark.dataSource.register(FakeDataSource)
    df = (
        spark.read.format("fake")
        .option("length", "1000")
        .option("partitions", "5")
        .load("/dev/faker")
    )
    df.limit(10).show()
    df.write.format("csv").mode("overwrite").save("./fakedata-02")
