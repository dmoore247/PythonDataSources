# PythonDataSources

Python DataSource classes for Spark 4.x

## Overview

This repository provides custom Python DataSource implementations for Apache Spark 4.x, enabling developers to create reusable data connectors that integrate seamlessly with Spark's DataFrame API[1]. These custom data sources allow you to define connections to various data systems and implement additional functionality beyond Spark's built-in connectors[3][5].

## Features

- **Custom Data Connectors**: Build reusable data sources for any external system or API
- **Spark 4.x Compatibility**: Designed specifically for Apache Spark 4.x using the new Python DataSource API
- **Batch and Streaming Support**: Implement both batch and streaming data sources
- **Easy Integration**: Register and use custom data sources with familiar Spark DataFrame syntax

## Requirements

- Apache Spark 4.x
- Python 3.8+
- PySpark

## Getting Started

### Basic DataSource Implementation

To create a custom data source, subclass the `DataSource` base class and implement the required methods[5]. This is the `zipdcm` data source as example:

```python
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

class ZipDCMDataSource(DataSource):
    @classmethod
    def name(cls):
        return "zipdcm"
    
    def schema(self):
        return "rowid INT, zipfile STRING, dcmfile STRING, meta STRING"
    
    def reader(self, schema: StructType):
        return ZipDCMDataSourceReader(schema, self.options)
```

### Required Methods

| Method | Description |
|--------|-------------|
| `name()`         | Required. The name of the data source |
| `schema()`       | Required. The schema of the data source to be read or written |
| `reader()`       | Must return a `DataSourceReader` for batch reading |
| `writer()`       | Must return a `DataSourceWriter` for batch writing |
| `streamReader()` | Must return a `DataSourceStreamReader` for streaming |
| `streamWriter()` | Must return a `DataSourceStreamWriter` for streaming output |

### Registration and Usage

After implementing your data source, register it with Spark and use it like any built-in data source[5]:

```python
spark_session = (
    SparkSession.builder.appName("PySpark DCM Zips Datasource Tester")
    .master("local[*]")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "1g")
    .getOrCreate()
)
spark_session.dataSource.register(ZipDCMDataSource)

# Read from your custom data source
df = spark.read.format("zipdcm").load("path/to/zipfiles or zipfile.zip")
df.show()

```

## Examples

This repository includes various example implementations demonstrating different use cases:

- **API Data Sources**: Connect to REST APIs and web services
- **File Format Parsers**: Custom file format readers and writers
- **Database Connectors**: Specialized database connection implementations
- **Streaming Sources**: Real-time data ingestion examples

## Architecture

### DataSource Class Hierarchy

The PySpark DataSource API provides a base class with methods to create data readers and writers[3][5]. Your custom implementations must be serializable, meaning they should be dictionaries or nested dictionaries containing primitive types.

### Batch vs Streaming

- **Batch Sources**: Implement `reader()` and `writer()` methods for one-time data processing
- **Streaming Sources**: Implement `streamReader()` and `streamWriter()` methods for continuous data processing

## Contributing

Contributions are welcome! Please feel free to submit pull requests with new data source implementations, improvements to existing sources, or documentation updates.

## License

This project is open source. Please refer to the [LICENSE](./LICENSE) file for details.

## Resources

- [Apache Spark DataSource API Documentation](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [PySpark Custom Data Sources Guide](https://docs.databricks.com/pyspark/datasources.html)
- [Spark 4.x Release Notes](https://spark.apache.org/releases/)

## Support

For questions, issues, or feature requests, please open an issue in this repository's issue tracker.
