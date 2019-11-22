
# OAP - Optimized Analytics Package for Spark Platform
[![Build Status](https://travis-ci.org/Intel-bigdata/OAP.svg?branch=master)](https://travis-ci.org/Intel-bigdata/OAP)

OAP - Optimized Analytics Package (previously known as Spinach) is designed to accelerate Ad-hoc query. OAP defines a new parquet-like columnar storage data format and offers a fine-grained hierarchical cache mechanism in the unit of "Fiber" in memory. What’s more, OAP has extended the Spark SQL DDL to allow user to define the customized indices based on relation.
## Building

```
mvn clean -q -Ppersistent-memory -DskipTests package
```
Must specify Profile `persistent-memory` when using Intel DCPMM.

## Running Test

To run all the tests, use
```
mvn clean -q -Ppersistent-memory test
```
To run any specific test suite, for example `OapDDLSuite`, use
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
To run test suites using `LocalClusterMode`, please refer to `SharedOapLocalClusterContext`

**NOTE**: Log level of OAP unit tests currently default to ERROR, please override src/test/resources/log4j.properties if needed.

## Prerequisites
You should have [Apache Spark](http://spark.apache.org/) of version 2.3.2 installed in your cluster
. Refer to Apache Spark's [documents](http://spark.apache.org/docs/2.3.2/) for details.
# Get started With OAP
In Yarn mode :
1. Build OAP find `oap-<version>-with-<spark-version>.jar` in `target/`
2. Deploy `oap-<version>-with-<spark-version>.jar` to master machine.
3. Put below configurations to _$SPARK_HOME/conf/spark-defaults.conf_
```
spark.files                         file:///path/to/oap-dir/oap-<version>-with-<spark-version>.jar
spark.executor.extraClassPath       ./oap-<version>-with-<spark-version>.jar
spark.driver.extraClassPath         /path/to/oap-dir/oap-<version>-with-<spark-version>.jar
spark.memory.offHeap.enabled        true
spark.memory.offHeap.size           20g
```
4. Run spark by `bin/spark-sql`, `bin/spark-shell`, `sbin/start-thriftserver` or `bin/pyspark` and try our examples

**NOTE**: 1. For spark standalone mode, you have to put `oap-<version>-with-<spark-version>.jar` to both driver and executor since `spark.files` is not working. Also don't forget to update `extraClassPath`.
          2. For yarn mode, we need to config all spark.driver.memory, spark.memory.offHeap.size and spark.yarn.executor.memoryOverhead (should be close to offHeap.size) to enable fiber cache.
          3. The comprehensive guidance and example of OAP configuration can be referred @https://github.com/Intel-bigdata/OAP/wiki/OAP-User-guide. Briefly speaking, the recommended configuration is one executor per one node with fully memory/computation capability.

## Example
```
./bin/spark-shell
> spark.sql(s"""CREATE TEMPORARY TABLE oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path 'hdfs:///oap-data-dir/')""".stripMargin)
> val data = (1 to 300).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
> spark.sql("drop oindex index1 on oap_test")
```
For a more detailed examples with performance compare, you can refer to [this page](https://github.com/Intel-bigdata/OAP/wiki/OAP-examples) for further instructions.

## Features

* Index - BTREE, BITMAP
Index is an optimization that is widely used in traditional databases. We are adopting 2 most used index types in OAP project.
BTREE index(default in 0.2.0) is intended for datasets that has a lot of distinct values, and distributed randomly, such as telephone number or ID number.
BitMap index is intended for datasets with a limited total amount of distinct values, such as state or age.
* Statistics - MinMax, Bloom Filter
Sometimes, reading index could bring extra cost for some queries, for example if we have to read all the data after all since there's no valid filter. OAP will automatically write some statistic data to index files, depending on what type of index you are using. With statistics, we can make sure we only use index if we can possibly boost the execution.
* Fine-grained cache
OAP format data file consists of several row groups. For each row group, we have many different columns according to user defined table schema. Each column data in one row is called a "Fiber", we are using this as the minimum cache unit.
* Parquet Data Adaptor
Parquet is the most popular and recommended data format in Spark open-source community. Since a lot of potential users are now using Parquet storing their data, it would be expensive for them to shift their existing data to OAP. So we designed the compatible layer to allow user to create index directly and use OAP cache on top of parquet data. With the Parquet reader we implemented, query over indexed Parquet data is also accelerated, though not as much as OAP.
* Orc Data Adaptor
Orc is another popular data format. We also designed the compatible layer to allow user to create index directly and use OAP cache on top of Orc data. With the Orc reader we implemented, query over indexed Orc data is also accelerated.
* Support DCPMM (Intel Optane DC Persistent Memory Module) as memory cache.

# OAP User guide
Refer to [OAP User guide](./docs/OAP-User-Guide.md) for more details.

# Integration with Spark
Although OAP (Optimized Analytical Package for Spark) acts as a plugin jar to Spark, there are still a few tricks to note when integration with Spark. 
Refer to [Spark OAP Integration Guide](./docs/Spark-OAP-Integration-Guide.md) for more details.

## Query Example and Performance Data
Take 1 simple ad-hoc query as instance, the store_sales table comes from TPCDS with data scale 200G. Generally we can see over 10x boost in performance.

#### First, create index:

"create oindex store_sales_ss_customer_sk_index on store_sales (ss_customer_sk) using btree"
#### query

1. "SELECT * FROM store_sales WHERE ss_customer_sk < 10000 AND ss_list_price < 100.0 AND ss_net_paid > 500.0"

Cases:                | T1/ms | T2/ms | T3/ms | Median/ms 
------------------    | ----- | ----- | ----- | ---------
Orc with index| 14964 |15023  | 15509 |   15023 
Orc with index, oap cache enabled| 3999| 7528| 8898|     7528
Orc with index, data cache separation enabled| 9633|11148| 8294|     9633
Orc without index|19253|20023|19140|    19253
Orc without index, oap cache enabled| 3056| 6147| 2508|     3056
oap with index|10152|  913| 6144|     6144|
oap with index, oap cache enabled| 1714|  850|  709|      850|
oap with index, data cache separation enabled|  931|  800|17429|      931|
oap without index| 5937|11780| 5373|     5937|
oap without index, oap cache enabled| 6102| 8307| 5172|     6102|
parquet with index, oap cache disabled|14971|15204|13748|    14971|
parquet with index, oap cache enabled| 4894| 1918| 4419|     4419|
parquet without index, oap cache disabled|15500|15881|14346|    15500|
parquet without index, oap cache enabled| 4768| 5041| 1636|     4768|
parquet with index, data cache separation enabled| 5052| 2169| 5499|     5052|

## Contributors
@JkSelf (Ke Jia, Intel) @jerrychenhf (Chen Haifeng, Intel) @LuciferYang (Yang Jie, Baidu) @lidinghao (Li Hao, Baidu) @shaowenzh (shaowen zhang, Intel) @WinkerDu (Du Ripeng, Baidu) @zhixingheyi-tian (Shen Xiangxiang, Intel) @WangGuangxin

## How to Contribute
If you are looking for some ideas on what to contribute, check out GitHub issues for this project labeled ["Pick me up!"](https://github.com/Intel-bigdata/OAP/issues?labels=pick+me+up%21&state=open).
Comment on the issue with your questions and ideas.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
* Please make sure to add the license headers to all new files. You can do this automatically by using the `mvn license:format` command.
* Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
* Give your operators some room. Not `a+b` but `a + b` and not `def foo(a:Int,b:Int):Int` but `def foo(a: Int, b: Int): Int`.
* Generally speaking, stick to the [Scala Style Guide](http://docs.scala-lang.org/style/)
* Make sure tests pass!



