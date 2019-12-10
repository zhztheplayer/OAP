cmake_minimum_required(VERSION 3.11)

project(spark_columnar_jni)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 14)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE  "Release")

option(TESTS "Build the tests" OFF)
option(DEBUG "Enable Debug Info" OFF)

find_package(JNI REQUIRED)

set(source_root_directory ${CMAKE_CURRENT_SOURCE_DIR})

if(TESTS)
  find_package(GTest)
macro(package_add_test TESTNAME)
  add_executable(${TESTNAME} ${ARGN})
  target_link_libraries(${TESTNAME} gtest gmock gtest_main spark_columnar_jni ${CMAKE_THREAD_LIBS_INIT})
  target_include_directories(${TESTNAME} PUBLIC ${source_root_directory})
  gtest_discover_tests(${TESTNAME}
    WORKING_DIRECTORY ${PROJECT_DIR}
    PROPERTIES VS_DEBUGGER_WORKING_DIRECTORY "${PROJECT_DIR}"
  )
  set_target_properties(${TESTNAME} PROPERTIES FOLDER tests)
endmacro()
  include(GoogleTest)
  ENABLE_TESTING()
  #add_custom_target(test ${CMAKE_CTEST_COMMAND} --output-on-failure)
  add_subdirectory(tests)
endif()

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native")
if(DEBUG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -DDEBUG -DDEBUG_LEVEL_1 -DDEBUG_LEVEL_2")
endif()
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

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(spark_columnar_jni SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
target_link_libraries(spark_columnar_jni ${ARROW_LIB} ${GANDIVA_LIB} ${GANDIVA_PROTOBUF_LIB})
target_include_directories(spark_columnar_jni PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${source_root_directory})
set_target_properties(spark_columnar_jni PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

install(TARGETS spark_columnar_jni
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
