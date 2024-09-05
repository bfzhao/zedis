package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesHandler extends RedisCommandHandler {
    TimeseriesHandler(InMemorySharedStore sharedStore) {
        super("Timeseries", sharedStore);

        commands.put("TS.ADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.ADD key timestamp value   [RETENTION retentionPeriod]   [ENCODING [COMPRESSED|UNCOMPRESSED]]   [CHUNK_SIZE size]   [ON_DUPLICATE policy]   [LABELS {label value}...]",
                        "Append a sample to a time series"
                )
        );
        commands.put("TS.ALTER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.ALTER key   [RETENTION retentionPeriod]   [CHUNK_SIZE size]   [DUPLICATE_POLICY policy]   [LABELS [{label value}...]]",
                        "Update the retention, chunk size, duplicate policy, and labels of an existing time series"
                )
        );
        commands.put("TS.CREATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.CREATE key   [RETENTION retentionPeriod]   [ENCODING [UNCOMPRESSED|COMPRESSED]]   [CHUNK_SIZE size]   [DUPLICATE_POLICY policy]   [LABELS {label value}...]",
                        "Create a new time series"
                )
        );
        commands.put("TS.CREATERULE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.CREATERULE sourceKey destKey   AGGREGATION aggregator bucketDuration   [alignTimestamp]",
                        "Create a compaction rule"
                )
        );
        commands.put("TS.DECRBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.DECRBY key subtrahend   [TIMESTAMP timestamp]   [RETENTION retentionPeriod]   [UNCOMPRESSED]   [CHUNK_SIZE size]   [LABELS {label value}...]",
                        "Decrease the value of the sample with the maximum existing timestamp, or create a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given decrement"
                )
        );
        commands.put("TS.DEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.DEL key fromTimestamp toTimestamp",
                        "Delete all samples between two timestamps for a given time series"
                )
        );
        commands.put("TS.DELETERULE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.DELETERULE sourceKey destKey",
                        "Delete a compaction rule"
                )
        );
        commands.put("TS.GET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.GET key   [LATEST]",
                        "Get the sample with the highest timestamp from a given time series"
                )
        );
        commands.put("TS.INCRBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.INCRBY key addend   [TIMESTAMP timestamp]   [RETENTION retentionPeriod]   [UNCOMPRESSED]   [CHUNK_SIZE size]   [LABELS {label value}...]",
                        "Increase the value of the sample with the maximum existing timestamp, or create a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given increment"
                )
        );
        commands.put("TS.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.INFO key   [DEBUG]",
                        "Returns information and statistics for a time series"
                )
        );
        commands.put("TS.MADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.MADD {key timestamp value}...",
                        "Append new samples to one or more time series"
                )
        );
        commands.put("TS.MGET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label...] FILTER filterExpr...",
                        "Get the sample with the highest timestamp from each time series matching a specific filter"
                )
        );
        commands.put("TS.MRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.MRANGE fromTimestamp toTimestamp  [LATEST]  [FILTER_BY_TS ts...]  [FILTER_BY_VALUE min max]  [WITHLABELS | SELECTED_LABELS label...]  [COUNT count]  [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]  FILTER filterExpr...  [GROUPBY label REDUCE reducer]",
                        "Query a range across multiple time series by filters in forward direction"
                )
        );
        commands.put("TS.MREVRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.MREVRANGE fromTimestamp toTimestamp  [LATEST]  [FILTER_BY_TS TS...]  [FILTER_BY_VALUE min max]  [WITHLABELS | SELECTED_LABELS label...]  [COUNT count]  [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]  FILTER filterExpr...  [GROUPBY label REDUCE reducer]",
                        "Query a range across multiple time-series by filters in reverse direction"
                )
        );
        commands.put("TS.QUERYINDEX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.QUERYINDEX filterExpr...",
                        "Get all time series keys matching a filter list"
                )
        );
        commands.put("TS.RANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.RANGE key fromTimestamp toTimestamp  [LATEST]  [FILTER_BY_TS ts...]  [FILTER_BY_VALUE min max]  [COUNT count]   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]",
                        "Query a range in forward direction"
                )
        );
        commands.put("TS.REVRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TS.REVRANGE key fromTimestamp toTimestamp  [LATEST]  [FILTER_BY_TS TS...]  [FILTER_BY_VALUE min max]  [COUNT count]  [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]",
                        "Query a range in reverse direction"
                )
        );
    }
}
