package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class TransactionsHandler extends RedisCommandHandler {
    TransactionsHandler(InMemorySharedStore sharedStore) {
        super("Transactions", sharedStore);

        commands.put("DISCARD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "DISCARD",
                        "Discards a transaction."
                )
        );
        commands.put("EXEC",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "EXEC",
                        "Executes all commands in a transaction."
                )
        );
        commands.put("MULTI",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MULTI",
                        "Starts a transaction."
                )
        );
        commands.put("UNWATCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "UNWATCH",
                        "Forgets about watched keys of a transaction."
                )
        );
        commands.put("WATCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "WATCH key [key ...]",
                        "Monitors changes to keys to determine the execution of a transaction."
                )
        );
    }
}
