create database itx2_events_demo_kostya;


create table itx2_events_demo_kostya.itx2_events_demo
(
    id    UInt64,
    key   UInt64,
    value UInt64
) engine = MergeTree ORDER BY id
SETTINGS index_granularity = 8192;

select timezone(), version()
