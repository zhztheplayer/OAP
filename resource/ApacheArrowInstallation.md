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

apache arrow and gandiva
``` shell
cd arrow/cpp/build
cmake -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON ..
make -j
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
