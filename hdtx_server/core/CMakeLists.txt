 

set(CONNECTION_SRC
        connection/meta_manager.cc
        connection/qp_manager.cc
        )

set(DTX_SRC
        dtx/doorbell.cc
        dtx/dtx_check.cc
        dtx/dtx_check_ro.cc
        dtx/dtx_check_rw.cc
        dtx/dtx_issue.cc
        dtx/dtx.cc
        dtx/dtx_exe_commit.cc
        dtx/dtx_local_meta.cc
        dtx/dtx_compare.cc
        dtx/dtx_compare_issue.cc
        dtx/dtx_compare_check.cc
        lock/lock.cc
        )

set(SCHEDULER_SRC
        scheduler/corotine_scheduler.cc
        )

add_library(HDTX STATIC
        ${CONNECTION_SRC}
        ${DTX_SRC}
        ${SCHEDULER_SRC}
        )

set_target_properties(HDTX PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(HDTX rlib pthread boost_coroutine boost_context boost_system)