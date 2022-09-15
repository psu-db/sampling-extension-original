cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(namespace "lsm")
project("lsm_sampling" VERSION 0.1.0)

add_library(${PROJECT_NAME} STATIC)
add_executable(pagedfile_tests ${CMAKE_CURRENT_SOURCE_DIR}/tests/pagedfile_tests.cpp)
target_link_libraries(pagedfile_tests PUBLIC ${PROJECT_NAME} check subunit)

target_sources(${PROJECT_NAME} 
    PRIVATE 
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ds/IsamTree.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/io/PagedFile.cpp
)

target_include_directories(${PROJECT_NAME} 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(${PROJECT_NAME} PUBLIC -g)
target_compile_options(pagedfile_tests PUBLIC -g)


