cmake_minimum_required(VERSION 3.16.3)
project(AFS C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/common" ${CMAKE_MODULE_PATH})

include(common/common.cmake)

add_compile_options(-O3)

get_filename_component(hw_proto "protos/payload.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/payload.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/payload.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/payload.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/payload.grpc.pb.h")

add_custom_command(
        OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
        DEPENDS "${hw_proto}")

include_directories(${FUSE_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
add_library(hw_grpc_proto
        ${hw_grpc_srcs}
        ${hw_grpc_hdrs}
        ${hw_proto_srcs}
        ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})

#### Client
add_library(cache-manager afs-client/cache-manager.cpp)
add_library(grpc-client afs-client/grpc-client.cpp)
target_link_libraries(
        grpc-client
        hw_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})

add_executable(perf-test afs-client/perf-test.cpp)
add_executable(perf-test-B afs-client/perf-test-B.cpp)
add_executable(perf-test-write afs-client/perf-test-write.cpp)
add_executable(perf-test-write-B afs-client/perf-test-write-B.cpp)
add_executable(afs-client afs-client/afs-client.cpp)
target_link_libraries(
        afs-client
        grpc-client
        cache-manager
        ${FUSE_LIBRARY}
        hw_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
)

#### Server
add_executable(afs-server afs-server/server.cc)
target_link_libraries(
        afs-server
        hw_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
)