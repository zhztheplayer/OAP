project(columnar_codegen_jni)

find_package(JNI REQUIRED)

set(JNI_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

set(SOURCES jni_wrapper.cpp)

set(GANDIVA_LINK_LIBS protobuf::libprotobuf)

set(GANDIVA_JNI_SOURCES
    config_builder.cc
    config_holder.cc
    expression_registry_helper.cc
    jni_common.cc
    Types.pb.cc)

add_library(columnar_codegen_jni SHARED ${SOURCES} ${GANDIVA_JNI_SOURCES} )
