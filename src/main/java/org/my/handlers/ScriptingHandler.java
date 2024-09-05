package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class ScriptingHandler extends RedisCommandHandler {
    ScriptingHandler(InMemorySharedStore sharedStore) {
        super("Scripting", sharedStore);

        commands.put("EVAL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "EVAL script numkeys [key [key ...]] [arg [arg ...]]",
                        "Executes a server-side Lua script."
                )
        );
        commands.put("EVALSHA",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]",
                        "Executes a server-side Lua script by SHA1 digest."
                )
        );
        commands.put("EVALSHA_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]",
                        "Executes a read-only server-side Lua script by SHA1 digest."
                )
        );
        commands.put("EVAL_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]",
                        "Executes a read-only server-side Lua script."
                )
        );
        commands.put("FCALL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FCALL function numkeys [key [key ...]] [arg [arg ...]]",
                        "Invokes a function."
                )
        );
        commands.put("FCALL_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FCALL_RO function numkeys [key [key ...]] [arg [arg ...]]",
                        "Invokes a read-only function."
                )
        );
        commands.put("FUNCTION DELETE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION DELETE library-name",
                        "Deletes a library and its functions."
                )
        );
        commands.put("FUNCTION DUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION DUMP",
                        "Dumps all libraries into a serialized binary payload."
                )
        );
        commands.put("FUNCTION FLUSH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION FLUSH [ASYNC | SYNC]",
                        "Deletes all libraries and functions."
                )
        );
        commands.put("FUNCTION KILL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION KILL",
                        "Terminates a function during execution."
                )
        );
        commands.put("FUNCTION LIST",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION LIST [LIBRARYNAME library-name-pattern] [WITHCODE]",
                        "Returns information about all libraries."
                )
        );
        commands.put("FUNCTION LOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION LOAD [REPLACE] function-code",
                        "Creates a library."
                )
        );
        commands.put("FUNCTION RESTORE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION RESTORE serialized-value [FLUSH | APPEND | REPLACE]",
                        "Restores all libraries from a payload."
                )
        );
        commands.put("FUNCTION STATS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "FUNCTION STATS",
                        "Returns information about a function during execution."
                )
        );
        commands.put("SCRIPT DEBUG",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCRIPT DEBUG",
                        "Sets the debug mode of server-side Lua scripts."
                )
        );
        commands.put("SCRIPT EXISTS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCRIPT EXISTS sha1 [sha1 ...]",
                        "Determines whether server-side Lua scripts exist in the script cache."
                )
        );
        commands.put("SCRIPT FLUSH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCRIPT FLUSH [ASYNC | SYNC]",
                        "Removes all server-side Lua scripts from the script cache."
                )
        );
        commands.put("SCRIPT KILL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCRIPT KILL",
                        "Terminates a server-side Lua script during execution."
                )
        );
        commands.put("SCRIPT LOAD",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCRIPT LOAD script",
                        "Loads a server-side Lua script to the script cache."
                )
        );
    }
}
