cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "lsm")
project("lsm_sampling" VERSION 0.1.0)

set(debug true)
set(tests True)
set(bench false)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_compile_options(-march=native)
add_link_options(-latomic)

if (debug) 
    add_compile_options(-g -O0)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
else()
    add_compile_options(-O3)
endif()



add_library(${PROJECT_NAME} STATIC)

# Test build instructions
if (tests) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/tests")

    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/tests/data/lsmtree")
    add_executable(pagedfile_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pagedfile_tests.cpp)
    target_link_libraries(pagedfile_tests PUBLIC ${PROJECT_NAME} check subunit atomic)
    target_compile_options(pagedfile_tests PUBLIC -llib)

    add_executable(memtable_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memtable_tests.cpp)
    target_link_libraries(memtable_tests PUBLIC ${PROJECT_NAME} check subunit atomic)
    target_compile_options(memtable_tests PUBLIC -llib)

    add_executable(inmemrun_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/inmemrun_tests.cpp)
    target_link_libraries(inmemrun_tests PUBLIC ${PROJECT_NAME} check subunit atomic)
    target_compile_options(inmemrun_tests PUBLIC -llib)

    add_executable(lsmtree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/lsmtree_tests.cpp)
    target_link_libraries(lsmtree_tests PUBLIC ${PROJECT_NAME} check subunit atomic)
    target_compile_options(lsmtree_tests PUBLIC -llib)

    add_executable(memlevel_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memlevel_tests.cpp)
    target_link_libraries(memlevel_tests PUBLIC ${PROJECT_NAME} check subunit atomic)
    target_compile_options(memlevel_tests PUBLIC -llib)

endif()

# Benchmark build instructions
if (bench)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  "${CMAKE_CURRENT_SOURCE_DIR}/bin/benchmarks")

    #add_executable(static_bench ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/static_bench.cpp)
    #target_link_libraries(static_bench PUBLIC ${PROJECT_NAME} atomic)
    #target_compile_options(static_bench PUBLIC -llib)
    #file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/data/static_bench")
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/utilities")
add_executable(data_generator ${CMAKE_CURRENT_SOURCE_DIR}/src/utilities/data_generator.cpp)

target_sources(${PROJECT_NAME} 
    PRIVATE 
        ${CMAKE_CURRENT_SOURCE_DIR}/src/io/PagedFile.cpp
)

target_include_directories(${PROJECT_NAME} 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(${PROJECT_NAME} PUBLIC gsl)

if (debug) 
    target_compile_options(${PROJECT_NAME} PUBLIC -g)
endif()



