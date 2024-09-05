package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class HyperloglogHandler extends RedisCommandHandler {
    HyperloglogHandler(InMemorySharedStore sharedStore) {
        super("Hyperloglog", sharedStore);

        commands.put("PFADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PFADD key [element [element ...]]",
                        "Adds elements to a HyperLogLog key. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("element")
                )
        );
        commands.put("PFCOUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PFCOUNT key [key ...]",
                        "Returns the approximated cardinality of the set(s) observed by the HyperLogLog key(s)."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("PFDEBUG",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PFDEBUG subcommand key",
                        "Internal commands for debugging HyperLogLog values."
                )
        );
        commands.put("PFMERGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PFMERGE destkey [sourcekey [sourcekey ...]]",
                        "Merges one or more HyperLogLog values into a single key."
                        , Command.Part.ofValue("destkey")
                        , Command.Part.ofListValue("sourcekey")
                )
        );
        commands.put("PFSELFTEST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PFSELFTEST",
                        "An internal command for testing HyperLogLog values."
                )
        );
    }
}
