import logging

import pytest

from zip_dcm_utils import (BucketPartition, RangePartition,
                           _bucket_partition_handler, _path_handler)

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def test_path_handler_wrong_folder():
    with pytest.raises(FileNotFoundError):
        paths = _path_handler("./resources/wrongfolder")
        assert paths is not None
        assert len(paths) == 0


def test_bucket_partitions():
    paths = _path_handler("./resources/dcms")
    assert paths is not None
    assert len(paths) == 5
    partitions = _bucket_partition_handler(paths)
    assert partitions is not None
    assert len(partitions) > 0
    # assert partitions is None, f"partitions: {partitions}"
    print(f"# partitions: {len(partitions)}")
    for part in partitions:
        assert part.files <= 3
        print(part)


def test_path_handler_zip():
    paths = _path_handler("./resources/dcms/y.zip")
    assert paths is not None
    assert len(paths) == 1


def test_path_handler_folder():
    paths = _path_handler("./resources/dcms")
    assert paths is not None
    assert len(paths) == 5


def test_path_handler_dcm():
    paths = _path_handler("./resources/dcms/y/1-1.dcm")
    assert len(paths) == 1


def test_readzipdcm_by_bucketpartition_partition_single_zip():
    from pathlib import Path

    from zip_dcm_utils import _readzipdcm_by_bucketpartition

    zip_file_path = "./resources/dcms/y.zip"

    p = Path(zip_file_path)
    print(p)
    file_size = p.lstat().st_size

    part = BucketPartition()
    part.append(file_size, p)
    dicom_keys_filter: list[str] = []
    res = _readzipdcm_by_bucketpartition(part, dicom_keys_filter)
    results = [_ for _ in res]
    assert len(results) == 1
    assert results[0][1].endswith(".dcm")
    assert type(results[0][2]) == dict
    assert (
        results[0][2].get("0020000E")["Value"][0]
        == "3.5.574.1.3.9030958.6.376.2860280475000825621"
    )


def test_readzipdcm_single_zip():
    from zip_dcm_utils import _readzipdcm

    zip_file_path = "./resources/dcms/y/y.zip"
    part = RangePartition(0, 1)
    paths = [zip_file_path]
    dicom_keys_filter: list[str] = []
    res = _readzipdcm(part, paths, dicom_keys_filter)
    logger.debug(res)


def test_readzipdcm_single_dcm():
    from zip_dcm_utils import _readzipdcm

    zip_file_path = "./resources/dcms/y/1-1.dcm"
    part = RangePartition(0, 1)
    paths = [zip_file_path]
    dicom_keys_filter: list[str] = []
    res = _readzipdcm(part, paths, dicom_keys_filter)
    print(type([_ for _ in res]))


if __name__ == "__main__":
    test_readzipdcm_single_dcm()
