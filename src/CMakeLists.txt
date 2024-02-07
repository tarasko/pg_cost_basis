add_postgresql_extension(pg_cost_basis
  VERSION 1.0
  SOURCES common.h sfunc.h pg_allocator.h pg_cost_basis.c acb0.cpp acb.cpp fifo.cpp
  SCRIPTS pg_cost_basis--1.0.sql
)
