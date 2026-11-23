CREATE TABLE signoz_traces.distributed_signoz_index_v3 AS signoz_traces.signoz_index_v3
ENGINE = Distributed('test_shard_localhost', 'signoz_traces', 'signoz_index_v3', cityHash64(trace_id));
