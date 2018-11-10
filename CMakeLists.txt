find_package(RdKafka)
if (RdKafka_FOUND)
    file(GLOB HEADERS "include/eosio/data_plugin/*.hpp")
    file(GLOB_RECURSE CPPKAFKA_SRC "vendor/cppkafka/src/*.cpp")
    file(GLOB TYPES "*_types.cpp")
    file(GLOB PRODUCERS "*_producer.cpp")

    add_library(data_plugin
                data_plugin.cpp 
                types.cpp
                producers.cpp
                ${TYPES}
                ${PRODUCERS}
                ${CPPKAFKA_SRC}
                )

    target_include_directories(data_plugin PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vendor/cppkafka/include")
    target_include_directories(data_plugin PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vendor/cppkafka/include/cppkafka")
    target_include_directories(data_plugin PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/include")


    target_link_libraries(data_plugin RdKafka::rdkafka)
    target_link_libraries(data_plugin chain_plugin appbase)
else()
    message ("Cannot Found Rdkafka, Please install it")
endif()
