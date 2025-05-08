 

set(SERVER_SOURCE server.cc)
add_executable(mem_pool ${SERVER_SOURCE})
target_link_libraries(mem_pool tatp_db smallbank_db tpcc_db micro_db rlib)
