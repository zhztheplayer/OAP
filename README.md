# SparkColumnarPlugin

This project is to enable columnar processing operators for spark sql.

# installation

#### build and install a columnarSupported spark

git clone https://github.com/xuechendi/spark.git
cd spark
git checkout wip_columnar_optimize
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install

#### after spark installation to /root/.m2/repository, build this plugin
cd SparkColumnarPlugin
mvn package

#### spark configuration
cat spark-defaults.xml

spark.driver.extraClassPath /${path}/SparkColumnarPlugin/core/target/scala-2.12/jars/spark-columnarPlugin_2.12-3.0.0-SNAPSHOT.jar
spark.executor.extraClassPath /${path}/SparkColumnarPlugin/core/target/scala-2.12/jars/spark-columnarPlugin_2.12-3.0.0-SNAPSHOT.jar

org.apache.spark.example.columnar.enabled true
spark.sql.extensions com.intel.sparkColumnarPlugin.ColumnarPlugin

#### Benchmark

wip

# contact

chendi.xue@intel.com
jian.zhang@intel.com
