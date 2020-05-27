cmake_minimum_required(VERSION 3.14)
project(Assign1Group14 C)

set(CMAKE_C_STANDARD 99)
set(Sources dberror.c storage_mgr.c test_assign1_1.c)
set(Headers dberror.h test_helper.h storage_mgr.h test_assign1_1.c)
add_executable(superlib ${Sources} ${Headers})