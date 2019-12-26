# OAP User Guide

* [Prerequisites](#Prerequisites)
* [Getting Started with OAP](#Getting-Started-with-OAP)
* [Configuration for YARN Cluster Mode](#Configuration-for-YARN-Cluster-Mode)
* [Configuration for Spark Standalone Mode](#Configuration-for-Spark-Standalone-Mode)
* [Working with OAP Index](#Working-with-OAP-Index)
* [Working with OAP Cache](#Working-with-OAP-Cache)
* [Run TPC-DS Benchmark for OAP](#Run-TPC-DS-Benchmark-for-OAP)


## Prerequisites
Before getting started with OAP on Spark, you should have set up a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark which is built with YARN support. If you don't want to build Spark by yourself, we have a pre-built Spark-2.3.2, you can download [Spark-2.3.2]() and setup Spark on your working node.
## Getting Started with OAP
### Building OAP
We have a pre-built OAP, you can download [OAP-0.6.1 for Spark 2.3.2 jar](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/oap-0.6.0-with-spark-2.3.2.jar) to your working node and put the OAP jar to your working directory such as `/home/oap/jars/`. If you’d like to build OAP from source code, please refer to [Developer Guide](https://github.com/HongW2019/OAP-spark2.4.3/blob/master/docs/Developer-Guide.md) for the detailed steps.
### Spark Configurations for OAP
Users usually test and run Spark SQL or scala scripts in Spark Shell which launches Spark applications on YRAN with ***client*** mode. In this section, we will start with Spark Shell then introduce other use scenarios. 

Before you run ` . $SPARK_HOME/bin/spark-shell `, you need to configure Spark for OAP integration. You need to add or update the following configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.

```
spark.master                      yarn
spark.deploy-mode                 client
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar     # absolute path of OAP jar on your working node
spark.executor.extraClassPath     ./oap-0.6-with-spark-2.3.2.jar                  # relative path of OAP jar
spark.driver.extraClassPath       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar     # absolute path of OAP jar on your working node
```
### Verify Spark with OAP Integration 
After configuration, you can follow the below steps and verify the OAP integration is working using Spark Shell.

Step 1. Create a test data path on your HDFS. Take data path `hdfs:///user/oap/` for example.
```
hadoop fs -mkdir /user/oap/
```
Step 2. Launch Spark Shell using the following command on your working node.
```
. $SPARK_HOME/bin/spark-shell
```
Steps 3. In Spark Shell, execute the following commands to test OAP integration. 
```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
```
The test create an index on a table and then show the created index. If there is no error happens, it means the OAP jar is working with the configuration. The picture below is one example of a successfully run.

![Spark_shell_running_results](./docs/image/spark_shell_oap.png)

## Configuration for YARN Cluster Mode
Spark Shell and Thrift Sever run Spark application in ***client*** mode. While Spark Submit tool and Spark SQL CLI can run Spark application in ***client*** or ***cluster*** mode deciding by --deploy-mode parameter.  Spark SQL There are two deploy modes that can be used to launch Spark applications on YARN, ***client*** and ***cluster*** mode. The [#Getting Started with OAP] session has shown the configuraitons needed for ***client*** mode. If you are running Spark Submit tool or Spark SQL CLI in ***cluster***mode, you need to following the below configuation steps instead.

Before run `spark-submit` with ***cluster*** mode, you should add below OAP configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.
```
spark.master                      yarn
spark.deploy-mode                 cluster
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar        # absolute path on your working node    
spark.executor.extraClassPath     ./oap-0.6-with-spark-2.3.2.jar                     # relative path 
spark.driver.extraClassPath       ./oap-0.6-with-spark-2.3.2.jar                     # relative path
```

## Configuration for Spark Standalone Mode
In addition to running on the YARN cluster managers, Spark also provides a simple standalone deploy mode. If you are using Spark in Spark Standalone mode, you need to copy the oap jar to ALL the worker nodes. And then set the following configurations in “$SPARK_HOME/conf/spark-defaults” of ALL worker nodes. 
```
spark.sql.extensions               org.apache.spark.sql.OapExtensions
spark.executor.extraClassPath      /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path on worker nodes
spark.driver.extraClassPath        /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path on worker nodes
```

## Working with OAP Index

You can use SQL DDL(create/drop/refresh/check/show index) to try OAP index function, run Spark with the following example to try OAP index function with Spark shell.
```
. $SPARK_HOME/bin/spark-shell
```
### Index Creation
Step 1. Use `CREATE` to create a table with `parquet` file format on corresponding HDFS data path `hdfs:///user/oap/`.
```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
```
Step 2. Insert data into table `oap_test`.
```
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")
```
Step 3. Create index with OAP on `oap_test`
```
> spark.sql("create oindex index1 on oap_test (a)")
> spark.sql("show oindex from oap_test").show()
```
### Use OAP Index
```
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
```
### Drop index
```
> spark.sql("drop oindex index1 on oap_test")
```
For  more detailed examples on OAP performance comparation, you can refer to this [page](https://github.com/Intel-bigdata/OAP/wiki/OAP-examples) for further instructions.

## Working with OAP Cache

If you want to run OAP with cache function, there are two media types in OAP to cache hot data: DRAM and DCPMM. 

### Use DRAM Cache 
Step 1. Change some configurations in `$SPARK_HOME/conf/spark-defaults.conf`. 

```
spark.memory.offHeap.enabled                true
spark.memory.offHeap.size                   80g      # half of total memory size
spark.sql.oap.parquet.data.cache.enable     true     #for parquet fileformat
spark.sql.oap.orc.data.cache.enable         true     #for orc fileformat
```
Step 2. Run Spark ***ThriftServer***

You should run Spark ***ThriftServer*** with the beeline scripts to use OAP DRAM cache, ThriftServer launchs Spark applications which can cache hot data for a long time backstage, and it can also accept query requests from different clients at the same time.

To directly verify DRAM Cache function, we reuse table `oap_test` created in the [Working with OAP Index](#Working-with-OAP-Index).

When we run ```spark-shell``` to create table `oap_test`, `metastore_db` will be created, so we need to run Thrift JDBC server in the same directory of `metastore`.
```
. $SPARK_HOME/sbin/start-thriftserver.sh
```
Step3. Use beeline and Connect to the JDBC/ODBC server in beeline with:

```
./beeline -u jdbc:hive2://vsr211:10000       
```
vsr211 is hostname, so you need change to your hostname.
 
Step 4. Using DRAM Cache on table `oap_test`
When ***0: jdbc:hive2://vsr211:10000>*** shows up, then you can directly use table oap_test, which is stored in the `default` database.
```
> SHOW databases;
> USE default;
> SHOW tables;
> USE oap_test;
```
Step 5. Run query like
```
> SELECT * FROM oap_test WHERE a = 1;
> SELECT * FROM oap_test WHERE a = 2;
> SELECT * FROM oap_test WHERE a = 3;
...
```
Then you can find the cache metric with OAP TAB in the spark history Web UI.

![webUI](./image/webUI.png)



### Use DCPMM Cache 
#### Prerequisites
When you want to use DCPMM to cache hot data, you should follow the below steps.

Step 1. You need have DCPMM formatted and mounted on your clusters.

Step 2. Download [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libmemkind.so.0), [libnuma.so.1](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libnuma.so.1) to directory `/lib64/`(Centos) in each executor node.
##### Achieve NUMA binding
Step 3. Install numactl by `yum install numactl -y ` to achieve NUMA binding
##### Configurations for DCPMM 
Step 4. Create a file named “persistent-memory.xml” under "$SPARK_HOME/conf/" and set the “initialPath” of numa node in “persistent-memory.xml”. You can directly copy the following part only changing `/mnt/pmem0` `/mnt/pmem1` to your path to DCPMM.

```
<persistentMemoryPool>
  <!--The numa id-->
  <numanode id="0">
    <!--The initial path for Intel Optane DC persistent memory-->
    <initialPath>/mnt/pmem0</initialPath>
  </numanode>
  <numanode id="1">
    <initialPath>/mnt/pmem1</initialPath>
  </numanode>
</persistentMemoryPool>
```

Here we privide you with an example, this cluster consists of 2 worker nodes, per node has 2 pieces of 488GB DCPMM ; 
```
spark.executor.instances                                   4               # 2x of number of your worker nodes
spark.yarn.numa.enabled                                    true            # enable numa
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND               1
spark.memory.offHeap.enabled                               false
spark.speculation                                          false
spark.sql.oap.fiberCache.memory.manager                    pm              # use DCPMM as cache media
spark.sql.oap.fiberCache.persistent.memory.initial.size    450g            # ~90% of total available DCPMM per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   30g             # the left DCPMM per executor
spark.sql.oap.parquet.data.cache.enable                    true            # for parquet fileformat
spark.sql.oap.orc.data.cache.enable                        true            # for orc fileformat
```
You can also run Spark with the same following example as DRAM cache to try OAP cache function with DCPMM, then you can find the cache metric with OAP TAB in the spark history Web UI.

## Run TPC-DS Benchmark for OAP

The industry often chooses TPC-DS workload as the benchmark for Spark. We also select 9 TPC-DS I/O intensive queries as the benchmark for OAP.

We provide the [OAP-TPCDS-Benchmark-Package.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/OAP-TPCDS-Benchmark-Package.zip) to setup and run the benchmark for OAP.

#### Prerequisites

1. Need to install python 2.7+ in the environment
2. Download the [OAP-TPCDS-Benchmark-Package.zip](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/OAP-TPCDS-Benchmark-Package.zip)  and unzip
3. Copy OAP-TPCDS-Benchmark-Package/tools/tpcds-kits on all cluster executor nodes under the same path.

#### Generate TPC-DS Data

1. Modify several variables at the beginning of OAP-TPCDS-Benchmark-Package/scripts/genData.scala, according to the actual situation.


```
// data scale GB
val scale = 2
// data file format
val format = "parquet"
// cluster NameNode
val namenode = "bdpe833n1"
// root directory of location to create data in.
val rootDir = s"hdfs://${namenode}:9000/genData$scale"
// name of database to create.
val databasename = s"tpcds$scale"
// location of tpcds-kits on executor nodes
val tpcdskitsDir = "/opt/Beaver/tpcds-kit/tools"
```

2. Modify several variables of OAP-TPCDS-Benchmark-Package/scripts/run_gen_data.sh, according to the actual situation


```
#replace the $SPARK_HOME
SPARK_HOME=/opt/Beaver/spark-2.3.2-bin-Phive
#replace with the OAP-TPCDS-Benchmark-Package path
PACKAGE=./OAP-TPCDS-Benchmark-Package
```

3. Generate Data

```
sh OAP-TPCDS-Benchmark-Package/scripts/run_gen_data.sh
```


#### Run Benchmark Queries

1. Start the thriftserver service

Start the thriftserver service by "OAP-TPCDS-Benchmark-Package/scripts/spark_thrift_server_yarn_with_DCPMM.sh" using DCPMM as the cache media or by "OAP-TPCDS-Benchmark-Package/scripts/spark_thrift_server_yarn_with_DRAM.sh" using DRAM as the cache media. 

   i. No matter which script you use, you need to modify the variable SPARK_HOME and other Spark configuration items in the script according to the actual environment.
       
   ii. Start thriftserver
       
      
```
sh spark_thrift_server_yarn_with_DCPMM.sh
              or
sh spark_thrift_server_yarn_with_DRAM.sh
```
       
       
       
2. Run Queries
       
      i. Modify several variables of OAP-TPCDS-Benchmark-Package/scripts/run_beeline.py, according to the actual situation.
       
```
# replace the $SPARK_HOME
SPARK_HOME = '/home/spark-sql/spark-2.3.2'
# replace with node running thriftserver 
hostname = 'bdpe833n1'
# replace with the actual database to query 
database_name = 'tpcds2'
# replace with the OAP-TPCDS-Benchmark-Package path
PACKAGE='./OAP-TPCDS-Benchmark-Package'
```

    
   ii. Run the 9 I/O intensive queries.
       
```
python run_beeline.py
```
      
   When the queries end, you will see the result file result.json in the current executive directory

