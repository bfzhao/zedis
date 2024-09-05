package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class StreamHandler extends RedisCommandHandler {
    StreamHandler(InMemorySharedStore sharedStore) {
        super("Stream", sharedStore);

        commands.put("XACK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XACK key group id [id ...]",
                        "Returns the number of messages that were successfully acknowledged by the consumer group member of a stream."
                )
        );
        commands.put("XADD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XADD key [NOMKSTREAM] [ [= | ~] threshold  [LIMIT count]] field value [field value ...]",
                        "Appends a new message to a stream. Creates the key if it doesn't exist."
                )
        );
        commands.put("XAUTOCLAIM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XAUTOCLAIM key group consumer min-idle-time start [COUNT count]  [JUSTID]",
                        "Changes, or acquires, ownership of messages in a consumer group, as if the messages were delivered to as consumer group member."
                )
        );
        commands.put("XCLAIM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms]  [TIME unix-time-milliseconds] [RETRYCOUNT count] [FORCE] [JUSTID]  [LASTID lastid]",
                        "Changes, or acquires, ownership of a message in a consumer group, as if the message was delivered a consumer group member."
                )
        );
        commands.put("XDEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XDEL key id [id ...]",
                        "Returns the number of messages after removing them from a stream."
                )
        );
        commands.put("XGROUP CREATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XGROUP CREATE key group [MKSTREAM]  [ENTRIESREAD entries-read]",
                        "Creates a consumer group."
                )
        );
        commands.put("XGROUP CREATECONSUMER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XGROUP CREATECONSUMER key group consumer",
                        "Creates a consumer in a consumer group."
                )
        );
        commands.put("XGROUP DELCONSUMER",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XGROUP DELCONSUMER key group consumer",
                        "Deletes a consumer from a consumer group."
                )
        );
        commands.put("XGROUP DESTROY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XGROUP DESTROY key group",
                        "Destroys a consumer group."
                )
        );
        commands.put("XGROUP SETID",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XGROUP SETID key group [ENTRIESREAD entries-read]",
                        "Sets the last-delivered ID of a consumer group."
                )
        );
        commands.put("XINFO CONSUMERS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XINFO CONSUMERS key group",
                        "Returns a list of the consumers in a consumer group."
                )
        );
        commands.put("XINFO GROUPS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XINFO GROUPS key",
                        "Returns a list of the consumer groups of a stream."
                )
        );
        commands.put("XINFO STREAM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XINFO STREAM key [FULL [COUNT count]]",
                        "Returns information about a stream."
                )
        );
        commands.put("XLEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XLEN key",
                        "Return the number of messages in a stream."
                )
        );
        commands.put("XPENDING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XPENDING key group [[IDLE min-idle-time] start end count [consumer]]",
                        "Returns the information and entries from a stream consumer group's pending entries list."
                )
        );
        commands.put("XRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XRANGE key start end [COUNT count]",
                        "Returns the messages from a stream within a range of IDs."
                )
        );
        commands.put("XREAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id  [id ...]",
                        "Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is available otherwise."
                )
        );
        commands.put("XREADGROUP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds]  [NOACK] STREAMS key [key ...] id [id ...]",
                        "Returns new or historical messages from a stream for a consumer in a group. Blocks until a message is available otherwise."
                )
        );
        commands.put("XREVRANGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XREVRANGE key end start [COUNT count]",
                        "Returns the messages from a stream within a range of IDs in reverse order."
                )
        );
        commands.put("XSETID",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XSETID key last-id [ENTRIESADDED entries-added]  [MAXDELETEDID max-deleted-id]",
                        "An internal command for replicating stream values."
                )
        );
        commands.put("XTRIM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "XTRIM key [= | ~] threshold [LIMIT count]",
                        "Deletes messages from the beginning of a stream."
                )
        );
    }
}
