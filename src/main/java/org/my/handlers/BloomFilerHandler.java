package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class BloomFilerHandler extends RedisCommandHandler {
    BloomFilerHandler(InMemorySharedStore sharedStore) {
        super("Bf", sharedStore);

        commands.put("BF.ADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.ADD key item",
                        "Adds an item to a Bloom Filter"
                )
        );
        commands.put("BF.CARD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.CARD key",
                        "Returns the cardinality of a Bloom filter"
                )
        );
        commands.put("BF.EXISTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.EXISTS key item",
                        "Checks whether an item exists in a Bloom Filter"
                )
        );
        commands.put("BF.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]",
                        "Returns information about a Bloom Filter"
                )
        );
        commands.put("BF.INSERT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.INSERT key [CAPACITY capacity] [ERROR error]  [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item  ...]",
                        "Adds one or more items to a Bloom Filter. A filter will be created if it does not exist"
                )
        );
        commands.put("BF.LOADCHUNK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.LOADCHUNK key iterator data",
                        "Restores a filter previously saved using SCANDUMP"
                )
        );
        commands.put("BF.MADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.MADD key item [item ...]",
                        "Adds one or more items to a Bloom Filter. A filter will be created if it does not exist"
                )
        );
        commands.put("BF.MEXISTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.MEXISTS key item [item ...]",
                        "Checks whether one or more items exist in a Bloom Filter"
                )
        );
        commands.put("BF.RESERVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.RESERVE key error_rate capacity [EXPANSION expansion]  [NONSCALING]",
                        "Creates a new Bloom Filter"
                )
        );
        commands.put("BF.SCANDUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BF.SCANDUMP key iterator",
                        "Begins an incremental save of the bloom filter"
                )
        );
    }
}
