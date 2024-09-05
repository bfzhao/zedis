package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class TopkHandler extends RedisCommandHandler {
    TopkHandler(InMemorySharedStore sharedStore) {
        super("Topk", sharedStore);

        commands.put("TOPK.ADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.ADD key items [items ...]",
                        "Increases the count of one or more items by increment"
                )
        );
        commands.put("TOPK.COUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.COUNT key item [item ...]",
                        "Return the count for one or more items are in a sketch"
                )
        );
        commands.put("TOPK.INCRBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.INCRBY key item increment [item increment ...]",
                        "Increases the count of one or more items by increment"
                )
        );
        commands.put("TOPK.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.INFO key",
                        "Returns information about a sketch"
                )
        );
        commands.put("TOPK.LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.LIST key [WITHCOUNT]",
                        "Return full list of items in Top K list"
                )
        );
        commands.put("TOPK.QUERY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.QUERY key item [item ...]",
                        "Checks whether one or more items are in a sketch"
                )
        );
        commands.put("TOPK.RESERVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOPK.RESERVE key topk [width depth decay]",
                        "Initializes a TopK with specified parameters"
                )
        );
    }
}
