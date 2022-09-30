cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "lsm")
project("lsm_sampling" VERSION 0.1.0)

set(debug True)
set(tests True)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

add_library(${PROJECT_NAME} STATIC)

# Test build instructions
if (tests) 
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/tests")

    add_executable(pagedfile_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pagedfile_tests.cpp)
    target_link_libraries(pagedfile_tests PUBLIC ${PROJECT_NAME} check subunit)
    target_compile_options(pagedfile_tests PUBLIC -g -llib)

    #add_executable(isamtree_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/isamtree_tests.cpp)
    #target_link_libraries(isamtree_tests PUBLIC ${PROJECT_NAME} check subunit)
    #target_compile_options(isamtree_tests PUBLIC -g -llib)

    add_executable(memtable_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/memtable_tests.cpp)
    target_link_libraries(memtable_tests PUBLIC ${PROJECT_NAME} check subunit)
    target_compile_options(memtable_tests PUBLIC -g -llib)
endif()



# Benchmark build instructions
#add_executable(pagedfile_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pagedfile_tests.cpp)
#target_link_libraries(pagedfile_tests PUBLIC ${PROJECT_NAME} check subunit)

#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin/bench")

target_sources(${PROJECT_NAME} 
    PRIVATE 
        #${CMAKE_CURRENT_SOURCE_DIR}/src/ds/IsamTree.cpp
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



