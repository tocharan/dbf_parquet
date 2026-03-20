cmake_minimum_required(VERSION 3.16)
project(dbf_parquet VERSION 1.0 LANGUAGES CXX)

# Use C++17 (Required by modern Apache Arrow)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. Define Sources
set(SOURCES
    src/main.cpp
    src/dbf.cpp
)

add_executable(dbf_parquet ${SOURCES})

# Include local headers
target_include_directories(dbf_parquet PRIVATE ${CMAKE_SOURCE_DIR}/src)

# ==========================================
# Apache Arrow Dependency
# ==========================================

# Find the Arrow package. 
# On Windows, ensure ARROW_ROOT is set or pass -DCMAKE_PREFIX_PATH="C:/path/to/arrow"
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
# Link Arrow to your executable
# Note: Arrow::arrow is the generic target that handles include paths and linking.
if(TARGET Arrow::arrow_shared)
    target_link_libraries(dbf_parquet PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
elseif(TARGET Arrow::arrow_static)
    target_link_libraries(dbf_parquet PRIVATE Arrow::arrow_static Parquet::parquet_static)
else()
    # Fallback for older CMake FindArrow configs
    target_link_libraries(dbf_parquet PRIVATE Arrow::arrow Parquet::parquet)
endif()

# ==========================================
# Platform Specific Configuration
# ==========================================

if(WIN32)
    # --- Windows Settings ---
    
    # MSVC (Visual Studio) specific flags
    if(MSVC)
        # Enable parallel build
        target_compile_options(dbf_parquet PRIVATE /MP)
        
        # Suppress Windows.h min/max macros (conflicts with std::)
        target_compile_definitions(dbf_parquet PRIVATE 
            WIN32_LEAN_AND_MEAN
            NOMINMAX
        )
    endif()

    # CRITICAL FOR WINDOWS:
    # If linking against Arrow DLLs (Shared Library), you must define ARROW_SHARED.
    # If linking against Arrow .lib (Static Library), define ARROW_STATIC.
    # The logic below attempts to auto-detect based on the target found above.
    if(TARGET Arrow::arrow_shared)
        target_compile_definitions(dbf_parquet PRIVATE ARROW_SHARED)
    else()
        target_compile_definitions(dbf_parquet PRIVATE ARROW_STATIC)
    endif()

elseif(UNIX AND NOT APPLE)
    # --- Linux Settings ---
    
    # Generally Linux finds Arrow via system paths (apt install libarrow-dev)
    # No extra definitions needed usually, but you can add Linux specific libs here if needed.
    # find_package(Threads REQUIRED)
    # target_link_libraries(dbf_parquet PRIVATE Threads::Threads)
endif()