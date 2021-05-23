create database itx2_events_demo_kostya;

create table itx2_events_demo_kostya.itx2_events_demo
(
    id    UInt64,
    key   VARCHAR(255),
    value VARCHAR(255)
) engine = MergeTree ORDER BY (id, key) PRIMARY KEY (id, key)
      SETTINGS index_granularity = 8192;

create table itx2_events_demo_kostya.itx2_events_with_array
(
    id          UInt64,
    key         VARCHAR(255),
    value       VARCHAR(255),
    array_index Int32
) engine = MergeTree ORDER BY (id, key, array_index) PRIMARY KEY (id, key, array_index)
      SETTINGS index_granularity = 8192;


select *
from itx2_events_demo_kostya.itx2_events_with_array
order by id desc, key asc
limit 500;

select timezone(), version();
