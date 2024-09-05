package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class SuggestionHandler extends RedisCommandHandler {
    SuggestionHandler(InMemorySharedStore sharedStore) {
        super("Suggestion", sharedStore);

        commands.put("FT.SUGADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SUGADD key string score   [INCR]   [PAYLOAD payload]",
                        "Adds a suggestion string to an auto-complete suggestion dictionary"
                )
        );
        commands.put("FT.SUGDEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SUGDEL key string",
                        "Deletes a string from a suggestion index"
                )
        );
        commands.put("FT.SUGGET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SUGGET key prefix   [FUZZY]   [WITHSCORES]   [WITHPAYLOADS]   [MAX max]",
                        "Gets completion suggestions for a prefix"
                )
        );
        commands.put("FT.SUGLEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FT.SUGLEN key",
                        "Gets the size of an auto-complete suggestion dictionary"
                )
        );
    }
}
