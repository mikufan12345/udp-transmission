cmake_minimum_required(VERSION 3.10)
project(orbbec_mjpeg LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# note: OB_SDK_ROOT may change if you reset the thing, just double check before you compile for no errors
# also ensure that you are using v2 SDK because you may get errors if not
set(OB_SDK_ROOT "/home/orb/Desktop/sdk/OrbbecSDK_v2.9.3_202607151523_2f6561c_linux_arm64")
set(OB_INCLUDE_DIR "${OB_SDK_ROOT}/include")
find_library(OB_LIBRARY NAMES OrbbecSDK obsensor PATHS "${OB_SDK_ROOT}/lib" "${OB_SDK_ROOT}/lib/aarch64" NO_DEFAULT_PATH)

add_executable(udp_streamer udp_server.cpp)
target_include_directories(udp_streamer PRIVATE ${OB_INCLUDE_DIR})

# pthread is required for std::thread and std::atomic
target_link_libraries(udp_streamer PRIVATE ${OB_LIBRARY} pthread)

# Embed RPATH so it finds the Orbbec .so file at runtime without LD_LIBRARY_PATH
set_target_properties(udp_streamer PROPERTIES
    BUILD_RPATH "${OB_SDK_ROOT}/lib"
    INSTALL_RPATH "${OB_SDK_ROOT}/lib"
    BUILD_WITH_INSTALL_RPATH TRUE
)

add_executable(list_profiles list_profiles.cpp)
target_include_directories(list_profiles PRIVATE ${OB_INCLUDE_DIR})
target_link_libraries(list_profiles PRIVATE ${OB_LIBRARY} pthread)

add_executable(get_intrinsics get_intrinsics.cpp)
target_include_directories(get_intrinsics PRIVATE ${OB_INCLUDE_DIR})
target_link_libraries(get_intrinsics PRIVATE ${OB_LIBRARY} pthread)
