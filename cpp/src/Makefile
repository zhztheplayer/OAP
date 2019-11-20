.PHONY:all
CC=g++

INCLUDE=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
INCLUDE+=-I$(shell pwd)
CFLAGS=-std=c++14 -fPIC -g -DDEBUG
LFLAGS=-L/usr/local/lib64
LIBS=-lgandiva -larrow -lgandiva_protobuf
MAIN=libspark_columnar_jni.so

all: clean $(MAIN) install

#test:
#		rm -f small_test
#			$(CC) $(CFLAGS) $(INCLUDE) -o small_test main.c /usr/lib64/libarrow_parquet_jni.so ${LIBS}

ARROW_COMPUTE_CODEGEN_SRC=codegen/arrow_compute/expr_visitor.cc codegen/arrow_compute/ext/kernels_ext.cc
COMPUTE_EXT_CODEGEN_SRC=
GANDIVA_CODEGEN_SRC=

CODEGEN_SRC=codegen/expr_visitor.cc
CODEGEN_SRC+=${ARROW_COMPUTE_CODEGEN_SRC}

JNI_SRC=jni/jni_wrapper.cc

$(MAIN):
		$(CC) $(CFLAGS) $(INCLUDE) -shared -o $(MAIN) ${JNI_SRC} ${CODEGEN_SRC} $(LIBS)

install:
		cp -f libspark_columnar_jni.so /usr/lib64/libspark_columnar_jni.so
		cp -f libspark_columnar_jni.so /usr/local/lib64/libspark_columnar_jni.so

clean:
		rm -f libspark_columnar_jni.so
