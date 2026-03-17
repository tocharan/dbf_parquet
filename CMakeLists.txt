cmake_minimum_required(VERSION 3.7)
project(dbf_parquet)


set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

add_executable(${PROJECT_NAME} src/main.cpp src/dbf.cpp)

find_package(Arrow CONFIG REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

find_package(Parquet CONFIG REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")

target_link_libraries(
    ${PROJECT_NAME} PRIVATE    
)