from pyspark.sql.datasource import DataSource, DataSourceReader
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


from typing import Iterator, List, Any


def _readzipdcm(
    partition: RangePartition, paths: list, dicom_keys_filter: list
) -> Iterator[List[Any]]:
    """
    Generator function to extract DICOM metadata from .dcm files within ZIP archives.

    Iterates over a partitioned list of ZIP file paths, opens each ZIP file, and processes files
    with a '.dcm' extension. For each DICOM file, reads the header metadata, removes specified
    large keys from the metadata dictionary, computes a SHA-1 hash of the pixel data, and yields
    the results as a list.

    Args:
        partition (RangePartition): An object with 'start' and 'end' attributes specifying the range of paths to process.
        paths (list): List of ZIP file paths to process.
        dicom_keys_filter (list): List of metadata keys to remove from the extracted DICOM metadata.

    Yields:
        list: A list containing:
            - rowid (int): Unique row identifier.
            - zip_file_path (str): Path to the ZIP file.
            - name_in_zip (str): Name of the DICOM file within the ZIP archive.
            - meta (dict): Filtered DICOM metadata dictionary with an added 'pixel_hash' key.

    Notes:
        - Assumes that the DICOM files can be read directly from the ZIP archive without extraction.
        - The 'pixel_hash' is computed using SHA-1 on the pixel array of the DICOM file.
        - Logging is performed at various steps for debugging purposes.
    """

    from zipfile import ZipFile
    from pydicom import dcmread
    import hashlib

    rowid = partition.start
    for zip_file_path in paths[partition.start : partition.end]:
        logger.debug(f"zipfile: {zip_file_path}")
        with ZipFile(zip_file_path, "r") as zipFile:
            for name_in_zip in zipFile.namelist():
                logger.debug(f" processing {zip_file_path}/{name_in_zip}")
                if len(name_in_zip) >= 3 and name_in_zip[-3:] == "dcm":
                    logger.debug(f" {name_in_zip} is a dcm file, reading")
                    with zipFile.open(name_in_zip, "r") as zip_fp:
                        with dcmread(zip_fp) as ds:
                            meta = ds.to_json_dict()
                            meta['hash'] = hashlib.sha1(zip_fp.read()).hexdigest()
                            meta["pixel_hash"] = hashlib.sha1(
                                ds.pixel_array
                            ).hexdigest()
                            if meta is None:
                                meta = ""
                            for key in dicom_keys_filter:
                                if key in meta:
                                    del meta[key]
                            # logger.debug(f"meta: {meta}")
                            rowid += 1
                            yield [rowid, str(zip_file_path), name_in_zip, meta]


def _path_handler(path: str, numPartitions: int) -> list:
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        logger.error(f"not exists {path}")
        raise Exception(f"Not Found {path}")  # TODO: Fix exception type

    # conflate either a direct zip file path or a dir into one case
    if p.is_dir():
        # a folder of zips
        # TODO: .glob() performance at extreme scales limits scale
        paths = sorted(Path(path).glob("**/*.zip"))
    else:
        paths = [path]

    length = len(paths)
    logger.debug(f"#zipfiles: {length}, #partitions:{numPartitions}, paths:{paths}")
    return paths


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

        #
        # In this implementation, we validate the path,
        # and get the list of the paths to scan.
        # TODO: Explore how to walk directory structures in parallel
        # TODO: Explore how to balance large skews in large archives vs. small. Current tests show 3-1 skew max v. median
        # TODO: Explore how to deal with large multi-frame DICOMs vs smaller single frame DICOMS (same amount of metadata)
        # TODO: Explore how to partition a single large Zip file
        #
        self.paths = _path_handler(self.path, self.numPartitions)

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
    formatter = logging.Formatter(
        "%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s"
    )
    # Create a handler (e.g., StreamHandler for console output)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    zip_file_path = "./resources/dcms"
    r = ZipDCMDataSourceReader(
        schema="rowid, int, x string, y string, z string",
        options={"path": zip_file_path, "numPartitions": 32},
    )
    partitions = r.partitions()
    logger.debug([_ for _ in partitions])

    for part in partitions:
        results = r.read(part)
        logger.debug([_ for _ in results])
