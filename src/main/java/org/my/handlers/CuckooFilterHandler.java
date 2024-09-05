package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class CuckooFilterHandler extends RedisCommandHandler {
    CuckooFilterHandler(InMemorySharedStore sharedStore) {
        super("Cf", sharedStore);

        commands.put("CF.ADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.ADD key item",
                        "Adds an item to a Cuckoo Filter"
                )
        );
        commands.put("CF.ADDNX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.ADDNX key item",
                        "Adds an item to a Cuckoo Filter if the item did not exist previously."
                )
        );
        commands.put("CF.COUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.COUNT key item",
                        "Return the number of times an item might be in a Cuckoo Filter"
                )
        );
        commands.put("CF.DEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.DEL key item",
                        "Deletes an item from a Cuckoo Filter"
                )
        );
        commands.put("CF.EXISTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.EXISTS key item",
                        "Checks whether one or more items exist in a Cuckoo Filter"
                )
        );
        commands.put("CF.INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.INFO key",
                        "Returns information about a Cuckoo Filter"
                )
        );
        commands.put("CF.INSERT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]",
                        "Adds one or more items to a Cuckoo Filter. A filter will be created if it does not exist"
                )
        );
        commands.put("CF.INSERTNX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]",
                        "Adds one or more items to a Cuckoo Filter if the items did not exist previously. A filter will be created if it does not exist"
                )
        );
        commands.put("CF.LOADCHUNK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.LOADCHUNK key iterator data",
                        "Restores a filter previously saved using SCANDUMP"
                )
        );
        commands.put("CF.MEXISTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.MEXISTS key item [item ...]",
                        "Checks whether one or more items exist in a Cuckoo Filter"
                )
        );
        commands.put("CF.RESERVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.RESERVE key capacity [BUCKETSIZE bucketsize]  [MAXITERATIONS maxiterations] [EXPANSION expansion]",
                        "Creates a new Cuckoo Filter"
                )
        );
        commands.put("CF.SCANDUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CF.SCANDUMP key iterator",
                        "Begins an incremental save of the bloom filter"
                )
        );
    }
}
