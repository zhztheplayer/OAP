cmake_minimum_required(VERSION 3.11)

project(spark_columnar_jni)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JNI REQUIRED)

find_library(ARROW_LIB arrow)
find_library(GANDIVA_LIB gandiva)
find_library(GANDIVA_PROTOBUF_LIB gandiva_protobuf)

# message("jni headers ${JNI_INCLUDE_DIRS}")
# message("arrow lib ${ARROW_LIB}")
# message("gandiva lib ${GANDIVA_LIB}")
# message("gandiva protobuf lib ${GANDIVA_PROTOBUF_LIB}")

if(NOT ARROW_LIB)
    message(FATAL_ERROR "Arrow library not found")
endif()

if(NOT GANDIVA_LIB)
    message(FATAL_ERROR "Gandiva library not found")
endif()

if(NOT GANDIVA_PROTOBUF_LIB)
    message(FATAL_ERROR "Gandiva protobuf library not found")
endif()

set(SPARK_COLUMNAR_PLUGIN_SRCS
        jni/jni_wrapper.cc
        codegen/expr_visitor.cc
        codegen/arrow_compute/expr_visitor.cc
        codegen/arrow_compute/ext/kernels_ext.cc
        )

set(SPARK_COLUMNAR_PLUGIN_TESTS
        codegen/test.cc
        )

add_library(spark_columnar_jni SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
target_link_libraries(spark_columnar_jni ${ARROW_LIB} ${GANDIVA_LIB} ${GANDIVA_PROTOBUF_LIB})
target_include_directories(spark_columnar_jni PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(codegen_test ${SPARK_COLUMNAR_PLUGIN_TESTS})
target_link_libraries(codegen_test spark_columnar_jni)

install(TARGETS spark_columnar_jni
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
