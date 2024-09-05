package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class TriggersAndFunctionsHandler extends RedisCommandHandler {
    TriggersAndFunctionsHandler(InMemorySharedStore sharedStore) {
        super("TriggersAndFunctions", sharedStore);

        commands.put("TFCALL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TFCALL. [ ...] [ ...]",
                        "Invoke a JavaScript function"
                )
        );
        commands.put("TFCALLASYNC",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TFCALLASYNC. [ ...] [ ...]",
                        "Invoke an asynchronous JavaScript function"
                )
        );
        commands.put("TFUNCTION DELETE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TFUNCTION DELETE &#34;&#34;",
                        "Delete a JavaScript library from Redis by name"
                )
        );
        commands.put("TFUNCTION LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TFUNCTION LIST [WITHCODE] [VERBOSE] [v] [LIBRARY]",
                        "List all JavaScript libraries loaded into Redis"
                )
        );
        commands.put("TFUNCTION LOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TFUNCTION LOAD [REPLACE] [CONFIG] &#34;&#34;",
                        "Load a new JavaScript library into Redis"
                )
        );
    }
}
