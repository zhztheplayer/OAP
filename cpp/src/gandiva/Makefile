.PHONY:all
CC=g++

CFLAGS=-std=c++11 -fPIC -g
LFLAGS=-L/usr/local/lib64
LIBS=-lgandiva -larrow -lprotobuf
MAIN=libgandiva_protobuf.so

all: clean $(MAIN) install

#test:
#		rm -f small_test
#			$(CC) $(CFLAGS) $(INCLUDE) -o small_test main.c /usr/lib64/libarrow_parquet_jni.so ${LIBS}

$(MAIN):
		$(CC) $(CFLAGS) $(INCLUDE) -shared -o $(MAIN) Types.pb.cc protobuf_utils.cc $(LIBS)

clean:
		rm -f ${MAIN}

install:
	  cp -rf ${MAIN} /usr/local/lib64
	  cp -rf ${MAIN} /usr/lib64
