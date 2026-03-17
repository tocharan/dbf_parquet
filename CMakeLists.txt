# CMakeLists.txt
cmake_minimum_required(VERSION 3.15)
project(dbf_parquet)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT APPLE)
    # set(CMAKE_CXX_CLANG_TIDY clang-tidy)
endif()

# Find dependencies provided by vcpkg (via vcpkg.cmake)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

set(
    PROJECT_SOURCES
    src/main.cpp 
    src/dbf.cpp
)

# main target
add_executable(${PROJECT_NAME} ${PROJECT_SOURCES})
target_link_libraries(${PROJECT_NAME} PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
target_link_libraries(${PROJECT_NAME} PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")

set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 17)

add_custom_target(
    check-format
    COMMAND clang-format --dry-run ${PROJECT_SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "checking format in src..."
)

add_custom_target(
    format
    COMMAND clang-format -i ${PROJECT_SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "checking format in src..."
)

