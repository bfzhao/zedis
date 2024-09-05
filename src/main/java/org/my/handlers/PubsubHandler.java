package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class PubsubHandler extends RedisCommandHandler {
    PubsubHandler(InMemorySharedStore sharedStore) {
        super("Pubsub", sharedStore);

        commands.put("PSUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PSUBSCRIBE pattern [pattern ...]",
                        "Listens for messages published to channels that match one or more patterns."
                )
        );
        commands.put("PUBLISH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBLISH channel message",
                        "Posts a message to a channel."
                )
        );
        commands.put("PUBSUB CHANNELS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBSUB CHANNELS [pattern]",
                        "Returns the active channels."
                )
        );
        commands.put("PUBSUB NUMPAT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBSUB NUMPAT",
                        "Returns a count of unique pattern subscriptions."
                )
        );
        commands.put("PUBSUB NUMSUB",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBSUB NUMSUB [channel [channel ...]]",
                        "Returns a count of subscribers to channels."
                )
        );
        commands.put("PUBSUB SHARDCHANNELS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBSUB SHARDCHANNELS [pattern]",
                        "Returns the active shard channels."
                )
        );
        commands.put("PUBSUB SHARDNUMSUB",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]",
                        "Returns the count of subscribers of shard channels."
                )
        );
        commands.put("PUNSUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "PUNSUBSCRIBE [pattern [pattern ...]]",
                        "Stops listening to messages published to channels that match one or more patterns."
                )
        );
        commands.put("SPUBLISH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SPUBLISH shardchannel message",
                        "Post a message to a shard channel"
                )
        );
        commands.put("SSUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SSUBSCRIBE shardchannel [shardchannel ...]",
                        "Listens for messages published to shard channels."
                )
        );
        commands.put("SUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SUBSCRIBE channel [channel ...]",
                        "Listens for messages published to channels."
                )
        );
        commands.put("SUNSUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SUNSUBSCRIBE [shardchannel [shardchannel ...]]",
                        "Stops listening to messages posted to shard channels."
                )
        );
        commands.put("UNSUBSCRIBE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "UNSUBSCRIBE [channel [channel ...]]",
                        "Stops listening to messages posted to channels."
                )
        );
    }
}
