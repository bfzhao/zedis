package org.my.handlers;

import org.my.Command;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

@Component
public class JsonHandler extends RedisCommandHandler {
    JsonHandler(InMemorySharedStore sharedStore) {
        super("Json", sharedStore);

        commands.put("JSON.ARRAPPEND",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRAPPEND key [path] value [value ...]",
                        "Append one or more json values into the array at path after the last element in it."
                )
        );
        commands.put("JSON.ARRINDEX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRINDEX key path value [start [stop]]",
                        "Returns the index of the first occurrence of a JSON scalar value in the array at path"
                )
        );
        commands.put("JSON.ARRINSERT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRINSERT key path index value [value ...]",
                        "Inserts the JSON scalar(s) value at the specified index in the array at path"
                )
        );
        commands.put("JSON.ARRLEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRLEN key [path]",
                        "Returns the length of the array at path"
                )
        );
        commands.put("JSON.ARRPOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRPOP key [path [index]]",
                        "Removes and returns the element at the specified index in the array at path"
                )
        );
        commands.put("JSON.ARRTRIM",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.ARRTRIM key path start stop",
                        "Trims the array at path to contain only the specified inclusive range of indices from start to stop"
                )
        );
        commands.put("JSON.CLEAR",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.CLEAR key [path]",
                        "Clears all values from an array or an object and sets numeric values to `0`"
                )
        );
        commands.put("JSON.DEBUG MEMORY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.DEBUG MEMORY key [path]",
                        "Reports the size in bytes of a key"
                )
        );
        commands.put("JSON.DEBUG",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.DEBUG",
                        "Debugging container command"
                )
        );
        commands.put("JSON.DEL",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.DEL key [path]",
                        "Deletes a value"
                )
        );
        commands.put("JSON.FORGET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.FORGET key [path]",
                        "Deletes a value"
                )
        );
        commands.put("JSON.GET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path  [path ...]]",
                        "Gets the value at one or more paths in JSON serialized form"
                )
        );
        commands.put("JSON.MERGE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.MERGE key path value",
                        "Merges a given JSON value into matching paths. Consequently, JSON values at matching paths are updated, deleted, or expanded with new children"
                )
        );
        commands.put("JSON.MGET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.MGET key [key ...] path",
                        "Returns the values at a path from one or more keys"
                )
        );
        commands.put("JSON.MSET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.MSET key path value [key path value ...]",
                        "Sets or updates the JSON value of one or more keys"
                )
        );
        commands.put("JSON.NUMINCRBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.NUMINCRBY key path value",
                        "Increments the numeric value at path by a value"
                )
        );
        commands.put("JSON.NUMMULTBY",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.NUMMULTBY key path value",
                        "Multiplies the numeric value at path by a value"
                )
        );
        commands.put("JSON.OBJKEYS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.OBJKEYS key [path]",
                        "Returns the JSON keys of the object at path"
                )
        );
        commands.put("JSON.OBJLEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.OBJLEN key [path]",
                        "Returns the number of keys of the object at path"
                )
        );
        commands.put("JSON.RESP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.RESP key [path]",
                        "Returns the JSON value at path in Redis Serialization Protocol (RESP)"
                )
        );
        commands.put("JSON.SET",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.SET key path value [NX | XX]",
                        "Sets or updates the JSON value at a path"
                )
        );
        commands.put("JSON.STRAPPEND",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.STRAPPEND key [path] value",
                        "Appends a string to a JSON string value at path"
                )
        );
        commands.put("JSON.STRLEN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.STRLEN key [path]",
                        "Returns the length of the JSON String at path in key"
                )
        );
        commands.put("JSON.TOGGLE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.TOGGLE key path",
                        "Toggles a boolean value"
                )
        );
        commands.put("JSON.TYPE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "JSON.TYPE key [path]",
                        "Returns the type of the JSON value at path"
                )
        );
    }
}
