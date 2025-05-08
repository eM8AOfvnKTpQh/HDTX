file(GLOB SOURCES "*.hpp")

include_directories(../include)
add_library(rlib STATIC ${SOURCES})
set_target_properties(rlib PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(rlib ibverbs pthread mlx4)