# ArrowDataSource for Apache Spark
A Spark DataSouce implementation for reading files into Arrow compatible columnar vectors.

## Build
### Preparations
#### Build and install Intel® optimized Arrow with Datasets Java API
git clone --branch native-sql-engine-clean https://github.com/Intel-bigdata/arrow.git

### Build This Library
mvn clean package
