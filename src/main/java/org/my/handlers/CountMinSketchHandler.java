package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class CountMinSketchHandler extends RedisCommandHandler {
    CountMinSketchHandler(InMemorySharedStore sharedStore) {
        super("Cms", sharedStore);

        commands.put("CMS.INCRBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.INCRBY key item increment [item increment ...]",
                        "Increases the count of one or more items by increment"
                )
        );
        commands.put("CMS.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.INFO key",
                        "Returns information about a sketch"
                )
        );
        commands.put("CMS.INITBYDIM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.INITBYDIM key width depth",
                        "Initializes a Count-Min Sketch to dimensions specified by user"
                )
        );
        commands.put("CMS.INITBYPROB",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.INITBYPROB key error probability",
                        "Initializes a Count-Min Sketch to accommodate requested tolerances."
                )
        );
        commands.put("CMS.MERGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight  [weight ...]]",
                        "Merges several sketches into one sketch"
                )
        );
        commands.put("CMS.QUERY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CMS.QUERY key item [item ...]",
                        "Returns the count for one or more items in a sketch"
                )
        );
    }
}
