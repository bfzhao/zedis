package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class ConnectionHandler extends RedisCommandHandler {
    ConnectionHandler(InMemorySharedStore sharedStore, ConnectionManager connectionManager) {
        super("Connection", sharedStore);

        commands.put("AUTH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "AUTH [username] password",
                        "Authenticates the connection."
                )
        );
        commands.put("CLIENT CACHING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT CACHING",
                        "Instructs the server whether to track the keys in the next request."
                )
        );
        commands.put("CLIENT GETNAME",
                new Command(
                        (ctx, args) -> {
                            String name = connectionManager.getName(ctx.getClientKey());
                            return name == null? RespType.NullBulkString() : RespType.ofBulkString(name);
                        },
                        "CLIENT GETNAME",
                        "Returns the name of the connection."
                )
        );
        commands.put("CLIENT GETREDIR",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofLong(connectionManager.getId(ctx.getClientKey()));
                        },
                        "CLIENT GETREDIR",
                        "Returns the client ID to which the connection's tracking notifications are redirected."
                )
        );
        commands.put("CLIENT ID",
                new Command(
                        (ctx, args) -> RespType.ofLong(connectionManager.getId(ctx.getClientKey())),
                        "CLIENT ID",
                        "Returns the unique client ID of the connection."
                )
        );
        commands.put("CLIENT INFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT INFO",
                        "Returns information about the connection."
                )
        );
        commands.put("CLIENT KILL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT KILL] | [USER username] | [ADDR ip",
                        "Terminates open connections."
                )
        );
        commands.put("CLIENT LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT LIST [TYPE ]  [ID client-id [client-id ...]]",
                        "Lists open connections."
                )
        );
        commands.put("CLIENT NO-EVICT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT NO-EVICT",
                        "Sets the client eviction mode of the connection."
                )
        );
        commands.put("CLIENT NO-TOUCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT NO-TOUCH",
                        "Controls whether commands sent by the client affect the LRU/LFU of accessed keys."
                )
        );
        commands.put("CLIENT PAUSE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT PAUSE timeout [WRITE | ALL]",
                        "Suspends commands processing."
                )
        );
        commands.put("CLIENT REPLY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT REPLY",
                        "Instructs the server whether to reply to commands."
                )
        );
        commands.put("CLIENT SETINFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT SETINFO",
                        "Sets information specific to the client or connection."
                )
        );
        commands.put("CLIENT SETNAME",
                new Command(
                        (ctx, args) -> {
                            String connectionName = args.valueWithName("connection-name");
                            connectionManager.setName(ctx.getClientKey(), connectionName);
                            return RespType.OK();
                        },
                        "CLIENT SETNAME connection-name",
                        "Sets the connection name."
                        , Command.Part.ofValue("connection-name")
                )
        );
        commands.put("CLIENT TRACKING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT TRACKING [REDIRECT client-id] [PREFIX prefix  [PREFIX prefix ...]] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]",
                        "Controls server-assisted client-side caching for the connection."
                )
        );
        commands.put("CLIENT TRACKINGINFO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT TRACKINGINFO",
                        "Returns information about server-assisted client-side caching for the connection."
                )
        );
        commands.put("CLIENT UNBLOCK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT UNBLOCK client-id [TIMEOUT | ERROR]",
                        "Unblocks a client blocked by a blocking command from a different connection."
                )
        );
        commands.put("CLIENT UNPAUSE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "CLIENT UNPAUSE",
                        "Resumes processing commands from paused clients."
                )
        );
        commands.put("ECHO",
                new Command(
                        (ctx, args) -> RespType.ofBulkString(args.valueWithName("message")),
                        "ECHO message",
                        "Returns the given string."
                        , Command.Part.ofValue("message")
                )
        );
        commands.put("HELLO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "HELLO [protover [AUTH username password] [SETNAME clientname]]",
                        "Handshakes with the Redis server."
                )
        );
        commands.put("PING",
                new Command(
                        (ctx, args) -> {
                            String message = args.optionValueAnonymous();
                            return RespType.ofBulkString(message == null? "PONG" : message);
                        },
                        "PING [message]",
                        "Returns the server's liveliness response."
                        , Command.Part.ofOptionAnonymous("message")
                )
        );
        commands.put("QUIT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "QUIT",
                        "Closes the connection."
                )
        );
        commands.put("RESET",
                new Command(
                        (ctx, args) -> {
                            connectionManager.reset(ctx.getClientKey());
                            return RespType.ofString("RESET");
                        },
                        "RESET",
                        "Resets the connection."
                )
        );
        commands.put("SELECT",
                new Command(
                        (ctx, args) -> {
                            int index = Integer.parseInt(args.valueWithName("index"));
                            if (index >= InMemorySharedStore.MAX_DB_SIZE || index < 0)
                                throw new IllegalArgumentException("db index is out of range");

                            connectionManager.selectDb(ctx.getClientKey(), index);
                            return RespType.OK();
                        },
                        "SELECT index",
                        "Changes the selected database."
                        , Command.Part.ofValue("index")
                )
        );
    }
}
