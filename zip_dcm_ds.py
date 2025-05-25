import logging
from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from zip_dcm_utils import RangePartition, _path_handler, _readzipdcm

logger = logging.getLogger(__file__)


class ZipDCMDataSourceReader(DataSourceReader):
    """
    Facilitate reading Zipfiles full of DCM (DICOM) files.
    """

    def __init__(self, schema, options):
        logger.debug(f"ZipDCMDataSourceReader(schema: {schema}, options: {options})")
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get("path", None)
        self.numPartitions = int(self.options.get("numPartitions", 2))
        self.deep = False
        self.paths = None

        # DICOM header keys to delete before saving the metadata
        self.dicom_keys_filter = ["60003000", "7FE00010", "00283010", "00283006"]
        assert self.path is not None

        self.paths = _path_handler(self.path)

    def partitions(self):
        """
        Compute 'splits' of the data to read
            self.paths is the list of files discovered and now need to be partitioned.
        """
        logger.debug(
            f"ZipDCMDataSourceReader.partitions({self.numPartitions}, {self.path}, paths: {self.paths}): "
        )
        length = len(self.paths)
        partitions = []
        partition_size_max = int(max(1, length / self.numPartitions))
        start = 0
        while start < length:
            end = min(length, start + partition_size_max)
            partitions.append(RangePartition(start, end))
            start = start + partition_size_max
        logger.debug(f"#partitions {len(partitions)} {partitions}")
        return partitions

    def read(self, partition) -> Iterator:
        """
        Executor level method, performs read by Range Partition
        """
        logger.debug(
            f"ZipDCMDataSourceReader.read({partition},{self.path}, paths:{self.paths}):"
        )

        assert self.path is not None, f"path: {self.path}"
        assert self.paths is not None, f"path: {self.path}"

        # Library imports must be within the method.
        return _readzipdcm(partition, self.paths, self.dicom_keys_filter)


class ZipDCMDataSource(DataSource):
    """
    A data source for batch query over zipped DICOM files the `ZipFile` and `PyDicom` libraries.
    """

    @classmethod
    def name(cls):
        datasource_type = "zipdcm"
        logger.debug(f"ZipDCMDataSource.name({datasource_type}): ")
        return datasource_type

    def schema(self):
        schema = "rowid INT, path STRING, meta STRING"
        logger.debug(f"ZipDCMDataSource.schema({schema}): ")
        return schema

    def reader(self, schema: StructType):
        logger.debug(f"ZipDCMDataSource.reader({schema}, options={self.options}): ")
        return ZipDCMDataSourceReader(schema, self.options)


if __name__ == "__main__":
    from zip_dcm_ds_test import test_ZipDCMDataSourceReader

    test_ZipDCMDataSourceReader()
