llvm-7.0: 
Arrow Gandiva depends on LLVM, and I noticed current version strictly depends on llvm7.0 if you installed any other version rather than 7.0, it will fail.
``` shell
wget http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
tar xf llvm-7.0.1.src.tar.xz
cd llvm-7.0.1.src/
cd tools
wget http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz
tar xf cfe-7.0.1.src.tar.xz
mv cfe-7.0.1.src clang
cd ..
mkdir build
cd build
cmake ..
cmake --build . -j
cmake --build . --target install
# check if clang has also been compiled, if no
cd tools/clang
mkdir build
cd build
cmake ..
make -j
make install
```

re2
``` shell
make
make test
make install
```

cmake: 
Arrow will download package during compiling, in order to support SSL in cmake, build cmake is optional.
``` shell
wget https://github.com/Kitware/CMake/releases/download/v3.15.0-rc4/cmake-3.15.0-rc4.tar.gz
tar xf cmake-3.15.0-rc4.tar.gz
cd cmake-3.15.0-rc4/
./bootstrap --system-curl --parallel=64 #parallel num depends on your server core number
make -j
make install
cmake --version
cmake version 3.15.0-rc4
```

double-conversion:
Arrow parquet will need this lib
``` shell
git clone https://github.com/google/double-conversion.git
cd double-conversion
mkdir build
cd build
cmake -DBUILD_SHARED_LIBS=ON ..
make
make install
```

apache arrow, parquet and gandiva
``` shell
git clone https://github/xuechendi/arrow
git checkout wip_hdfs_parquet_reader
cd arrow/cpp/build
cmake -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON ..
make
make install

# build java
cd ../java
# change arrow.cpp.build.dir to the relative path of cpp build dir, 
# noted: since gandiva is subdir under java, so the relative path should add one more ../
mvn clean install -P arrow-jni -am -DskipTests -Darrow.cpp.build.dir=../../cpp/build/release
```

run test
``` shell
mvn test -pl gandiva -P arrow-jni
```

Enable Arrow Parquet Reader
ArrowParquetReader is a currently ongoing feature we are working on and provides a Java Api to read parquet data from Hdfs into Arrow as ArrowRecordBatch or List[FieldVector], by integrate this feature into Spark by using SparkColumnarPlugin, read performance is evaluated to be improved by 1.5x.
``` shell
# copy libhdfs3.so to /usrl/lib64/
cd arrow/cpp/src/jni/parquet/
make
cd ../../../../../java #arrow/java
mvn clean install -P arrow-jni -am -DskipTests -Darrow.cpp.build.dir=../../cpp/build/release
```

run test
There is a small java test to check if your codes are working
``` shell
cd arrow/java
hadoop classpath --jar hadoop.jar
java -cp /root/.m2/repository/commons-cli/commons-cli/1.4/commons-cli-1.4.jar:hadoop.jar:/root/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:/root/.m2/repository/io/netty/netty-all/4.1.30.Final/netty-all-4.1.30.Final.jar:/root/.m2/repository/org/apache/arrow/arrow-adapter-builder/1.0.0-SNAPSHOT/arrow-adapter-builder-1.0.0-SNAPSHOT.jar:/root/.m2/repository/org/apache/arrow/arrow-vector/1.0.0-SNAPSHOT/arrow-vector-1.0.0-SNAPSHOT.jar:/root/.m2/repository/org/apache/arrow/arrow-memory/1.0.0-SNAPSHOT/arrow-memory-1.0.0-SNAPSHOT.jar:/root/.m2/repository/org/apache/arrow/arrow-format/1.0.0-SNAPSHOT/arrow-format-1.0.0-SNAPSHOT.jar:/root/.m2/repository/com/google/protobuf/protobuf-java/3.7.1/protobuf-java-3.7.1.jar:/root/.m2/repository/com/google/flatbuffers/flatbuffers-java/1.9.0/flatbuffers-java-1.9.0.jar org.apache.arrow.adapter.builder.ParquetReaderTest --path hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2452642/part-00196-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet --numColumns 10

Will open file hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2452642/part-00196-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet
Read Batches 437, total length is 1787201
testParquetReader completed
```
