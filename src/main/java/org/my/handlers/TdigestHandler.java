package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class TdigestHandler extends RedisCommandHandler {
    TdigestHandler(InMemorySharedStore sharedStore) {
        super("Tdigest", sharedStore);

        commands.put("TDIGEST.ADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.ADD key value [value ...]",
                        "Adds one or more observations to a t-digest sketch"
                )
        );
        commands.put("TDIGEST.BYRANK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.BYRANK key rank [rank ...]",
                        "Returns, for each input rank, an estimation of the value (floating-point) with that rank"
                )
        );
        commands.put("TDIGEST.BYREVRANK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.BYREVRANK key reverse_rank [reverse_rank ...]",
                        "Returns, for each input reverse rank, an estimation of the value (floating-point) with that reverse rank"
                )
        );
        commands.put("TDIGEST.CDF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.CDF key value [value ...]",
                        "Returns, for each input value, an estimation of the fraction (floating-point) of (observations smaller than the given value + half the observations equal to the given value)"
                )
        );
        commands.put("TDIGEST.CREATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.CREATE key [COMPRESSION compression]",
                        "Allocates memory and initializes a new t-digest sketch"
                )
        );
        commands.put("TDIGEST.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.INFO key",
                        "Returns information and statistics about a t-digest sketch"
                )
        );
        commands.put("TDIGEST.MAX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.MAX key",
                        "Returns the maximum observation value from a t-digest sketch"
                )
        );
        commands.put("TDIGEST.MERGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.MERGE destination-key numkeys source-key [source-key ...]  [COMPRESSION compression] [OVERRIDE]",
                        "Merges multiple t-digest sketches into a single sketch"
                )
        );
        commands.put("TDIGEST.MIN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.MIN key",
                        "Returns the minimum observation value from a t-digest sketch"
                )
        );
        commands.put("TDIGEST.QUANTILE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.QUANTILE key quantile [quantile ...]",
                        "Returns, for each input fraction, an estimation of the value (floating point) that is smaller than the given fraction of observations"
                )
        );
        commands.put("TDIGEST.RANK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.RANK key value [value ...]",
                        "Returns, for each input value (floating-point), the estimated rank of the value (the number of observations in the sketch that are smaller than the value + half the number of observations that are equal to the value)"
                )
        );
        commands.put("TDIGEST.RESET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.RESET key",
                        "Resets a t-digest sketch"
                )
        );
        commands.put("TDIGEST.REVRANK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.REVRANK key value [value ...]",
                        "Returns, for each input value (floating-point), the estimated reverse rank of the value (the number of observations in the sketch that are larger than the value + half the number of observations that are equal to the value)"
                )
        );
        commands.put("TDIGEST.TRIMMED_MEAN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile",
                        "Returns an estimation of the mean value from the sketch, excluding observation values outside the low and high cutoff quantiles"
                )
        );
    }
}
