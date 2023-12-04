set(RUN_SRC run.cc)
add_executable(run ${RUN_SRC})
target_link_libraries(run worker)
