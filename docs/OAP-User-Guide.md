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
The test creates an index on a table and then show the created index. If there is no error happens, it means the OAP jar is working with the configuration. The picture below is one example of a successfully run.

![Spark_shell_running_results](./docs/image/spark_shell_oap.png)

## Configuration for YARN Cluster Mode
Spark Shell and Thrift Sever run Spark application in ***client*** mode. While Spark Submit tool and Spark SQL CLI can run Spark application in ***client*** or ***cluster*** mode deciding by --deploy-mode parameter.  Spark SQL There are two deploy modes that can be used to launch Spark applications on YARN, ***client*** and ***cluster*** mode. The [#Getting Started with OAP] session has shown the configuraitons needed for ***client*** mode. If you are running Spark Submit tool or Spark SQL CLI in ***cluster***mode, you need to following the below configuation steps instead.

Before run `spark-submit` with ***cluster*** mode, you should add below OAP configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.
```
spark.sql.extensions              org.apache.spark.sql.OapExtensions
spark.files                       /home/oap/jars/oap-0.6-with-spark-2.3.2.jar        # absolute path on your working node    
spark.executor.extraClassPath     ./oap-0.6-with-spark-2.3.2.jar                     # relative path 
spark.driver.extraClassPath       ./oap-0.6-with-spark-2.3.2.jar                     # relative path
```

## Configuration for Spark Standalone Mode
In addition to running on the YARN cluster managers, Spark also provides a simple standalone deploy mode. If you are using Spark in Spark Standalone mode, you need to copy the oap jar to ALL the worker nodes. And then set the following configurations in “$SPARK_HOME/conf/spark-defaults” on working node.
```
spark.sql.extensions               org.apache.spark.sql.OapExtensions
spark.executor.extraClassPath      /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path on worker nodes
spark.driver.extraClassPath        /home/oap/jars/oap-0.6-with-spark-2.3.2.jar      # absolute path on worker nodes
```

## Working with OAP Index

After a successful OAP integration, you can use OAP SQL DDL to manage table indexs. The DDL operations include index create, drop, refresh and show. You can run Spark Shell to try and test these functions. The below index examples based on an example table created by the following commands in Spark Shell.

```
> spark.sql(s"""CREATE TABLE oap_test (a INT, b STRING)
       USING parquet
       OPTIONS (path 'hdfs:///user/oap/')""".stripMargin)
> val data = (1 to 30000).map { i => (i, s"this is test $i") }.toDF().createOrReplaceTempView("t")
> spark.sql("insert overwrite table oap_test select * from t")       
```

### Index Creation
Use CREATE OINDEX DDL command to create an B+ Tree index or bitmap index with given name on a table column. 
```
CREATE OINDEX index_name ON table_name (column_name) USING [BTREE, BITMAP]
```
The following example creates an B+ Tree index on column "a" of oap_test table.

```
> spark.sql("create oindex index1 on oap_test (a)")
```
###
Use SHOW OINDEX command to show all the created indexs on a specified table. For example,
```
> spark.sql("show oindex from oap_test").show()
```
### Use OAP Index
Using index in query is transparent. When the SQL queries have filter conditions on the column(s) which can take advantage to use the index to filter the data scan, the index will be automatically applied to the execution of Spark SQL. The following example will automatically use the underlayer index created on column "a".
```
> spark.sql("SELECT * FROM oap_test WHERE a = 1").show()
```
### Drop index
Use DROP OINDEX command to drop a named index.
```
> spark.sql("drop oindex index1 on oap_test")
```
For more detailed examples on OAP performance comparation, you can refer to this [page](https://github.com/Intel-bigdata/OAP/wiki/OAP-examples) for further instructions.

## Working with OAP Cache

OAP is capable to provide input data cache functionality in executor. Considering to utilize the cache data among different SQL queries, we should configure to allow different SQL queries to use the same executor process. This can be achieved by running your queries through Spark ThriftServer. The below steps assume to use Spark ThriftServer. For cache media, we support both DRAM and Intel DCPMM which means you can choose to cache data in DRAM or Intel DCPMMM if you have DCPMM configured in hardware.

### Use DRAM Cache 
Step 1. Make the following configuration changes in Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf`. 

```
spark.memory.offHeap.enabled                true
spark.memory.offHeap.size                   80g      # half of total memory size
spark.sql.oap.parquet.data.cache.enable     true     #for parquet fileformat
spark.sql.oap.orc.data.cache.enable         true     #for orc fileformat
```
You should change the parameter spark.memory.offHeap.size value according to the availability of DRAM capacity to cache data.

Step 2. Launch Spark ***ThriftServer***
After configuration, you can launch Spark Thift Server. And use Beeline command line tool to connect to the Thrift Server to execute DDL or DML operations. And the data cache will automatically take effect for Parquet or ORC file sources. To help you to do a quick verification of cache functionality, below steps will reuse database metastore created in the [Working with OAP Index](#Working-with-OAP-Index) which contains `oap_test` table definition. In production, Spark Thrift Server will have its own metastore database directory or metastore service and use DDL's  through Beeline for creating your tables.

When you run ```spark-shell``` to create table `oap_test`, `metastore_db` will be created in the directory from which you run '$SPARK_HOME/bin/spark-shell'. Go the same directory you ran Spark Shell and then execute the following command to launch Thrift JDBC server.
```
. $SPARK_HOME/sbin/start-thriftserver.sh
```
Step3. Use Beeline and connect to the Thrift JDBC server using the following command, replacing the hostname (mythriftserver) with your own Thrift Server hostname.

```
./beeline -u jdbc:hive2://mythriftserver:10000       
```
After the connection is established, execute the following command to check the metastore is initialized correctly.

```
> SHOW databases;
> USE default;
> SHOW tables;
```
 
Step 4. Run queries on table which will use the cache automatically. For example,

```
> SELECT * FROM oap_test WHERE a = 1;
> SELECT * FROM oap_test WHERE a = 2;
> SELECT * FROM oap_test WHERE a = 3;
...
```
Step 5. To verify that the cache funtionality is in effect, you can open Spark History Web UI and go to OAP tab page. And check the cache metrics. The following picture is an example.

![webUI](./image/webUI.png)


### Use DCPMM Cache 
#### Prerequisites
Before configuring in OAP to use DCPMM cache, you need to make sure the following:

- DCPMM hardwares are installed, formatted and mounted correctly on every cluster worker nodes. You will get a mounted directory to use if you have done this. Usually, the DCPMM on each socket will be mounted as a directory. For example, on a two sockets system, we may get two mounted directories named `/mnt/pmem0` and `/mnt/pmem1`.

- [Memkind](http://memkind.github.io/memkind/) library has been installed on every cluster worker nodes. Please use the latest Memkind version. You can compile Memkind based on your system. We have a pre-build binary for x86 64bit CentOS Linux and you can download [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libmemkind.so.0) and [libnuma.so.1](https://github.com/Intel-bigdata/OAP/releases/download/v0.6.0-spark-2.3.2/libnuma.so.1) and put the files to `/lib64/` directory in each worker node in cluster.

##### Configure for NUMA
To achieve the optimum performance, we need to configure NUMA for binding executor to NUMA node and try access the right DCPMM device on the same NUMA node. You need install numactl on each worker node. For example, on CentOS, run following command to install numactl.

```yum install numactl -y ```

##### Configure for DCPMM 
Create a configuration file named “persistent-memory.xml” under "$SPARK_HOME/conf/" if it doesn't exist. Use below contents as a template and change the “initialPath” to your mounted paths for DCPMM devices. 

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
##### Configure for Spark/OAP to enable DCPMM cache
Make the following configuration changes in Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.executor.instances                                   6               # 2x of number of your worker nodes
spark.yarn.numa.enabled                                    true            # enable numa
spark.executorEnv.MEMKIND_ARENA_NUM_PER_KIND               1
spark.memory.offHeap.enabled                               false
spark.speculation                                          false
spark.sql.oap.fiberCache.memory.manager                    pm              # use DCPMM as cache media
spark.sql.oap.fiberCache.persistent.memory.initial.size    256g            # DCPMM capacity per executor
spark.sql.oap.fiberCache.persistent.memory.reserved.size   50g             # Reserved space per executor
spark.sql.oap.parquet.data.cache.enable                    true            # for parquet fileformat
spark.sql.oap.orc.data.cache.enable                        true            # for orc fileformat
```
You need to change the value for spark.executor.instances, spark.sql.oap.fiberCache.persistent.memory.initial.size, and spark.sql.oap.fiberCache.persistent.memory.reserved.size according to your real environment. Here we privide you with an example, this cluster consists of 2 worker nodes, per node has 2 pieces of 488GB DCPMM ; ou can also run Spark with the same following example as DRAM cache to try OAP cache function with DCPMM, then you can find the cache metric with OAP TAB in the spark history Web UI.

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

