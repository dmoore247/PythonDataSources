from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql.datasource import InputPartition

import sys
from typing import Iterator

import logging
logger = logging.getLogger(__file__)

class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end

class ZipDCMDataSourceReader(DataSourceReader):
    """ 
        Facilitate reading Zipfiles full of DCM (DICOM) files.
    """
    def __init__(self, schema, options):
        logger.debug(f"ZipDCMDataSourceReader(schema: {schema}, options: {options})")
        self.schema: StructType = schema
        self.options = options
        self.path = self.options.get('path', None)
        self.numPartitions = int(self.options.get('numPartitions',2))
        self.deep = False
        self.paths = None
     
        #DICOM header keys to delete before saving the metadata
        self.dicom_keys_filter = ["60003000", "7FE00010", "00283010", "00283006"]
        assert self.path is not None
        from pathlib import Path
        
        p = Path(self.path)

        #
        # In this implementation, we validate the path, 
        # and get the list of the paths to scan.
        # TODO: Explore how to walk directory structures in parallel
        # TODO: Explore how to balance large skews in large archives vs. small
        # TODO: Explore how to deal with large multi-frame DICOMs vs smaller single frame DICOMS (same amount of metadata)
        # TODO: Explore how to partition a single large Zip file
        #
        if not p.exists():
            logger.error(f"not exists {self.path}")
            raise Exception(f"Not Found {self.path}") #TODO: Fix exception type
            return [None, None, None]

        # conflate either a direct zip file path or a dir into one case
        if p.is_dir():
            # a folder of zips
            # TODO: .glob() performance at extreme scales limits scale
            self.paths = sorted(Path(self.path).glob("**/*.zip"))
        else:
            self.paths = [self.path]

        length = len(self.paths)
        logger.debug(f"#zipfiles: {length}, #partitions:{self.numPartitions}, paths:{self.paths}")

    def partitions(self):
        """
            Compute 'splits' of the data to read
                self.paths is the list of files discovered and now need to be partitioned.
        """
        logger.debug(f"ZipDCMDataSourceReader.partitions({self.numPartitions}, {self.path}, paths: {self.paths}): ")
        length = len(self.paths)
        partitions = []
        partition_size_max = int(max(1,length/self.numPartitions))
        start = 0
        while start < length:
            end = min(length, start+partition_size_max)
            partitions.append(RangePartition(start, end))
            start = start + partition_size_max
        logger.debug(f"#partitions {len(partitions)} {partitions}")
        return partitions

    def read(self, partition) -> Iterator:
        """
            Performs read by Range Partition
        """
        logger.debug(f"ZipDCMDataSourceReader.read({partition},{self.path}, paths:{self.paths}):")

        assert self.path is not None, f"path: {self.path}"
        assert self.paths is not None, f"path: {self.path}"

        # Library imports must be within the method.
        logger.debug(f"doing imports")
        from zipfile import ZipFile
        from pydicom import dcmread
        from pathlib import Path
        import hashlib
        deep = False

        #
        # for every zip file path, 
        #   for every file in the zip, 
        #       if it has the dcm extension, 
        #           open the dcm, 
        #               extract the header metadata
        #                   delete the large keys from the meta dictionary
        #                       yield
        rowid = partition.start
        for zip_file_path in self.paths[partition.start:partition.end]:
            logger.debug(f"zipfile: {zip_file_path}")
            with ZipFile(zip_file_path,'r') as zipFile:
                for name_in_zip in zipFile.namelist():
                    logger.debug(f" processing {zip_file_path}/{name_in_zip}")
                    if len(name_in_zip) >= 3 and name_in_zip[-3:] == "dcm":
                        logger.debug(f" {name_in_zip} is a dcm file, reading" )
                        with zipFile.open(name_in_zip, 'r') as zip_fp:
                            with dcmread(zip_fp) as ds:
                                meta = ds.to_json_dict()
                                meta['pixel_hash'] = hashlib.sha1(ds.pixel_array).hexdigest()
                                if meta is None:
                                    meta = ""
                                for key in self.dicom_keys_filter:
                                    if key in meta:
                                        del meta[key]
                                #logger.debug(f"meta: {meta}")
                                rowid += 1
                                yield [rowid, str(zip_file_path), name_in_zip, meta]

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
        schema = "rowid INT, zipfile STRING, dcmfile STRING, meta STRING"
        logger.debug(f"ZipDCMDataSource.schema({schema}): ")
        return schema

    def reader(self, schema: StructType):
        logger.debug(f"ZipDCMDataSource.reader({schema}, options={self.options}): ")
        return ZipDCMDataSourceReader(schema, self.options)


if __name__ == "__main__":
    """
        A small unit test, no spark.
    """
    import logging
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s')
    # Create a handler (e.g., StreamHandler for console output)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    zip_file_path = './resources/dcms'
    r = ZipDCMDataSourceReader(
        schema="rowid, int, x string, y string, z string",
        options={
        'path': zip_file_path, 
        'numPartitions': 32
        })
    partitions = r.partitions()
    logger.debug([_ for _ in partitions])

    for part in partitions:
        results = r.read(part)
        logger.debug([_ for _ in results])
