Below is the basic guide for a new Spark developer about how to use OAP with Spark. 
# Environment setup
## i. Configurations
With yarn, you need to especially set below properties to ensure all the available resources (CPU cores, memory) can be fully utilized and not be exceeded by the Spark executors with OAP.
```
yarn.nodemanager.vmem-pmem-ratio
yarn.nodemanager.resource.memory-mb
yarn.scheduler.minimum-allocation-mb
yarn.scheduler.maximum-allocation-mb
yarn.nodemanager.resource.cpu-vcores
yarn.scheduler.minimum-allocation-vcores
yarn.scheduler.maximum-allocation-vcores
```
`yarn.nodemanager.resource.memory-mb` and `yarn.nodemanager.resource.cpu-vcores` can be set to be the total available memory size and total available CPU cores of each worker. `yarn.scheduler.maximum-allocation-mb` is less than `yarn.nodemanager.resource.memory-mb`. `yarn.scheduler.maximum-allocation-vcores`
is less than `yarn.nodemanager.resource.cpu-vcores`. Meanwhile `spark.executor.memory` can be less equal to `yarn.scheduler.maximum-allocation-mb` and `spark.executor.cores` is less equal to `yarn.scheduler.maximum-allocation-vcores`. 
You need to ensure that the above properties are consistent among the driver and all the workers. If failed to launch Spark with OAP, you need to check the logs to find the reason.
## ii. How to deploy OAP
### a) With yarn
```
spark.files                        file:///{PATH_TO_OAP_JAR}/oap-0.X.0.jar
spark.executor.extraClassPath      ./oap-0.X.0.jar
spark.driver.extraClassPath        /{PATH_TO_OAP_JAR}/oap-0.X.0.jar
```
With the above approach, you just need to deploy OAP jar to the driver, and no need to deploy to the workers.
### b) With standalone mode
You need to manually deploy the OAP jar to both the master and workers.

# Important properties for OAP with Spark
## i. Below are critical properties for OAP index creation performance with Spark.
```
spark.executor.cores
spark.executor.instances
spark.executor.memory
spark.memory.offHeap.enabled # !!!MUST be true in oap 0.3.0.
spark.memory.offHeap.size
```
Executor instances can be 1~2X of work nodes. Considering the count of executor instances (`N`) on each node, executor memory can be around `1/N` of each worker total available memory. Usually each worker has one or two executor instances. However, considering the cache utilization, one executor per work node is recommended.
Always enable offHeap memory and set a reasonable (the larger the better) size, as long as OAP's fine-grained cache takes advantage of offHeap memory, otherwise user might encounter weird behaviors.
## ii. Below are critical properties for OAP query performance with Spark.
```
Rowgroup size
spark.yarn.executor.memoryOverhead #Important for yarn mode, as tasks would be killed if they use too much off-heap memory.
```
row group size and fiber cache size are OAP properties. They can be set with below approaches.
```
spark.createDataFrame(rawMapData).write.mode("overwrite").format("oap”).option(“rowgroup”, 1048576).save("/oap_rawMapData_1000")
```
row group size can be `1/1000` of total data size. 

The fiber cache size is configured by spark.memory.offHeap.size (0.35 of it by default) which is usually close to `spark.yarn.executor.memoryOverhead` for better query performance, meanwhile `spark.yarn.executor.memoryOverhead` can be around half of the executor memory size. 
Basically we have a rough calculation here: 
```
spark.memory.offHeap.size ~= spark.yarn.executor.memoryOverhead 
spark.executor.memory + spark.memory.offHeap.size(spark.yarn.executor.memoryOverhead) < yarn.nodemanager.resource.memory-mb 
spark.driver.memory/spark.driver.cores >= 3G
```

# Reference settings
Below are reference settings for OAP with Spark on yarn for the total 5 IA machines with 72 CPU cores Xeon E5-2699 v3 2.30G and 256G memory for each machine. Since the standalone mode will automatically utilize the available resources as fully as possible, it's easier for resource settings. For standalone mode, you can refer to below settings and do some minor changes accordingly. So far we haven't tried mesos mode. We expect no big changes with mesos mode.
```
spark.driver.cores                 64         # Use all cores.
spark.driver.memory                100G       # ~1/2 total for on-heap.
spark.executor.cores               32         # Use as many cores, but each core(task) should has at least 3G memory.
spark.executor.instances           4          # 1 executor per work node.
spark.executor.memory              100G       # ~1/2 total for on-heap.
spark.memory.offHeap.enabled       true       # Enable off-heap
spark.memory.offHeap.size          120G       # ~1/2 total for off-heap.
spark.yarn.executor.memoryOverhead 120G       # ~1/2 for yarn to control memory usage.
```

## Configurations and Performance Tuning

Parquet Support - Enable OAP support for parquet files
* Default: true
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_PARQUET_ENABLED.key, "false")`

Index Directory Setting - Enable OAP support to separate the index file in specific directory. The index file is in the directory of data file in default.
* Default: ""
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_INDEX_DIRECTORY.key, "/tmp")`
* Usage2: `SET spark.sql.oap.index.directory = /tmp`

Fiber Cache Size - Total Memory size to cache Fiber, configured implicitly by 'spark.memory.offHeap.size'
* Default Size: `spark.memory.offHeap.size * 0.7`
* Usage: Fiber cache locates in off heap storage memory, basically this size is spark.memory.offHeap.size * 0.7. But as execution can borrow a few memory from storage in UnifiedMemoryManager mode, it may vary during execution.

Full Scan Threshold - If the analysis result is above this threshold, it will go through the whole data file instead of read index data.
* Default: 0.8
* Usage: `sqlContext.conf.setConfString(SQLConf.OAP_FULL_SCAN_THRESHOLD.key, "0.8")`

Row Group Size - Row count for each row group
* Default: 1048576
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_ROW_GROUP_SIZE.key, "1048576")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('rowgroup' '1048576')`

Compression Codec - Choose compression type for OAP data files.
* Default: GZIP
* Values: UNCOMPRESSED, SNAPPY, GZIP, LZO (Note that ORC does not support GZIP)
* Usage1: `sqlContext.conf.setConfString(SQLConf.OAP_COMPRESSION.key, "SNAPPY")`
* Usage2: `CREATE TABLE t USING oap OPTIONS ('compression' 'SNAPPY')`

# Trouble shooting
## i.	Java object serial version incompatibility issue
If you are using spark-shell and see below exception, usually it’s caused by incorrectly deploying OAP jar to the workers. Please follow the above steps to correctly deploy OAP.
```
scala> java.io.InvalidClassException: org.apache.spark.Heartbeat; local class incompatible: stream classdesc serialVersionUID = -3395605122687888761, local class serialVersionUID = -7235392889710158116
```
## ii.	OAP index creation failed
If you are seeing below exception when trying to create OAP index, usually you are incorrectly using data frame and schema for OAP file. Please make sure save the schema you specified to the OAP file. After OAP file is loaded, do not change the schema any more.
```
scala> spark.sql("create sindex index1 on oapView(oapKey)")
org.apache.spark.sql.execution.datasources.OapException: We don't support index building for Project [_1#27 AS oapKey#38, _2#28 AS oapValue#39]
```
Below are the examples.
```
scala> val rawMapData = sc.parallelize(1 to 1000, 1000).map{i => (i, s"this is test $i")}
scala> spark.createDataFrame(rawMapData).write.mode("overwrite").format("oap").save("/oap_rawMapData_1000")
```
You can add schema as below before saving.
```
spark.createDataFrame(rawMapData).toDF(“oapKey”, “oapValue”).write.mode("overwrite").format("oap").save("/oap_rawMapData_1000")
```
Do not change schema as below after OAP file is loaded already.
```
scala> val dfOapRead = spark.read.format("oap").load("hdfs:///oap_rawMapData_1000")
scala> dfOapRead.toDF("oapKey", "oapValue").createOrReplaceTempView("oapView")
```
## iii. Index creation is time consuming. 
This step is usually GC intensive. You can add below GC options to check the each task specific GC behavior.
```
spark.executor.extraJavaOptions -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```
In this case, it needs to increase the initial executor memory size to be close to the task working set. For example, it can be half of the executor memory size using below example:
```
spark.executor.memory  35G #Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this option.
```
And meanwhile increase executor instances to fully utilize the existing workers. You can check spark Web UI to check that.
## iv. The query with filer is not as fast as expected.
In this case, user can increase `spark.memory.offHeap.size` to utilize the whole left system memory, see if more cache helps user out of the problem.









