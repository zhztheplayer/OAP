# SparkColumnarPlugin

This project is to enable columnar processing operators for spark sql, columnar processing will use Apache Arrow to allocate memory and evaluate with LLVM. 

## Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Benchmark](#benchmark)
- [Contact](#contact)

## Introduction

#### Two key concepts of this project:
1. Use arrow as column vector format as intermediate data among spark operator.
2. Use Gandiva to evaluate columnar operator expressions.

![Overview](/resource/overview.jpg)

## Installation

#### Build and install 1.11.0 Parquet, we will leverage Parquet-Arrow 1.11.0 version for convering Parquet Schema to Arrow Schema in spark. So make sure, Parquet-Arrow 1.11.0 installed in your mvn dependency path.

``` shell
wget http://archive.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz
tar xzf thrift-0.12.0.tar.gz
cd thrift-0.12.0
chmod +x ./configure
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
make install

yum install boost-devel
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs
make install -j

git clone https://github.com/apache/parquet-mr.git
git checkout apache-parquet-1.11.0
mvn clean install -pl parquet-arrow -am -DskipTests
ls /root/.m2/repository/org/apache/parquet/parquet-arrow/1.11.0/
parquet-arrow-1.11.0.jar  parquet-arrow-1.11.0.jar.lastUpdated  parquet-arrow-1.11.0.pom  parquet-arrow-1.11.0.pom.lastUpdated  parquet-arrow-1.11.0-tests.jar  _remote.repositories
```

#### Build and install a columnarSupported spark

``` shell
git clone https://github.com/xuechendi/spark.git
cd spark
git checkout wip_arrow_support_v2
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
```

#### Install Apache Arrow and Gandiva

Please refer this markdown to install Apache Arrow and Gandiva.
[Apache Arrow Installation](/resource/ApacheArrowInstallation.md)


#### Build this project

This project has dependencies of Apache Spark and Apache Arrow, so please make sure these two project has been installed under /root/.m2/repository/ before start build this project.
``` shell
cd SparkColumnarPlugin
mvn package
```

#### spark configuration

``` shell
cat spark-defaults.xml

spark.driver.extraClassPath /mnt/nvme2/chendi/SparkColumnarPlugin/core/target/core-1.0-jar-with-dependencies.jar:/mnt/nvme2/chendi/arrow/java/gandiva/target/arrow-gandiva-0.14.0.jar
spark.executor.extraClassPath /mnt/nvme2/chendi/SparkColumnarPlugin/core/target/core-1.0-jar-with-dependencies.jar:/mnt/nvme2/chendi/arrow/java/gandiva/target/arrow-gandiva-0.14.0.jar

org.apache.spark.example.columnar.enabled true
spark.sql.extensions com.intel.sparkColumnarPlugin.ColumnarPlugin
spark.sql.columnVector.arrow.enabled true
```

## Benchmark

For initial microbenchmark performance, we add 10 fields up with spark, data size is 200G data

![Performance](/resource/performance.png)

## contact

chendi.xue@intel.com
jian.zhang@intel.com
