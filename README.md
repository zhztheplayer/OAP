# SparkColumnarPlugin

This project is to enable columnar processing operators for spark sql, columnar processing will use Apache Arrow to allocate memory and evaluate with LLVM. 

## Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Benchmark](#benchmark)
- [Contact](#contact)

## Introduction

Two key concepts of this project:
1. Use arrow as column vector format as intermediate data among spark operator.
2. Use Gandiva to evaluate columnar operator expressions.

![Overview](/resource/overview.jpg)

## Installation

#### build and install a columnarSupported spark

``` shell
git clone https://github.com/xuechendi/spark.git
cd spark
git checkout wip_columnar_optimize
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
```

#### after spark installation to /root/.m2/repository, build this plugin

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
