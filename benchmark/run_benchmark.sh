#!/bin/bash

#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd test/target/benchmark-1.0-jar-with-dependencies.jar 10 2>spark.log

#hdfs dfs -rm -r /tpcds_output/web_sales
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 2 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 3 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 4 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 5 2>spark.log
#
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 6 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 7 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 8 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 9 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarBatchScan test/target/benchmark-1.0-jar-with-dependencies.jar 10 2>spark.log

spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 2 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 3 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 4 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 5 2>spark.log
#
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 6 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 7 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 8 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 9 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.ColumnarAdd target/benchmark-1.0-jar-with-dependencies.jar 10 2>spark.log

#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 1 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 2 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 3 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 4 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 5 2>spark.log
#
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 6 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 7 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 8 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 9 2>spark.log
#spark-submit --master yarn --deploy-mode client --class test.Add test/target/benchmark-1.0-jar-with-dependencies.jar 10 2>spark.log
