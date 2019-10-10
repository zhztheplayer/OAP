.PHONY:all
CC=g++

INCLUDE=-I${JAVA_HOME} -I${JAVA_HOME}/include/linux -I${JAVA_HOME}/include
CFLAGS=-std=c++11 -fPIC -g
LFLAGS=-L/usr/local/lib64
LIBS=-lgandiva -larrow -lgandiva_protobuf
MAIN=libspark_columnar_jni.so

all: clean $(MAIN) install

#test:
#		rm -f small_test
#			$(CC) $(CFLAGS) $(INCLUDE) -o small_test main.c /usr/lib64/libarrow_parquet_jni.so ${LIBS}

$(MAIN):
		$(CC) $(CFLAGS) $(INCLUDE) -shared -o $(MAIN) jni_wrapper.cc expr_visitor.cc arrow_compute_expr_visitor.cc $(LIBS)

install:
		cp -f libspark_columnar_jni.so /usr/lib64/libspark_columnar_jni.so
		cp -f libspark_columnar_jni.so /usr/local/lib64/libspark_columnar_jni.so

clean:
		rm -f libspark_columnar_jni.so
