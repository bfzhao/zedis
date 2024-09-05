package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.*;

@Component
public class GenericHandler extends RedisCommandHandler {
    GenericHandler(InMemorySharedStore sharedStore) {
        super("Generic", sharedStore);

        commands.put("COPY",
                new Command(
                        (ctx, args) -> {
                            String sk = args.valueWithName("source");
                            String dk = args.valueWithName("destination");
                            boolean replace = args.hasOption("REPLACE");
                            Map<String, ValueWithTTL> targetStore = ctx.getStore();
                            if (args.hasOption("DB")) {
                                int idx = Integer.parseInt(args.optionWithName("DB")[0]);
                                if (idx < 0 || idx >= InMemorySharedStore.MAX_DB_SIZE)
                                    throw new IllegalArgumentException("db index is out of range");
                                targetStore = sharedStore.getDB(idx);
                            }

                            if (targetStore.containsKey(dk) && replace)
                                targetStore.remove(dk);

                            targetStore.put(sk, ctx.getStore().get(sk));
                            return RespType.ofError("not implemented");
                        },
                        "COPY source destination [DB destination-db] [REPLACE]",
                        "Copies the value of a key to a new key."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofOptionNamedSimple("DB", "destination-db")
                        , Command.Part.ofOptionNamedSimple("REPLACE")
                )
        );
        commands.put("DEL",
                new Command(
                        (ctx, args) -> {
                            Long[] ret = new Long[1];
                            ret[0] = 0L;
                            String[] keys = args.valueListDefault();
                            Arrays.stream(keys).forEach(x -> {
                                ValueWithTTL v = ctx.getStore().remove(x);
                                if (v != null)
                                    ret[0]++;
                            });
                            return RespType.ofLong(ret[0]);
                        },
                        "DEL key [key ...]",
                        "Deletes one or more keys."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("DUMP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "DUMP key",
                        "Returns a serialized representation of the value stored at a key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("EXISTS",
                new Command(
                        (ctx, args) -> {
                            Long[] ret = new Long[1];
                            ret[0] = 0L;
                            String[] keys = args.valueListDefault();
                            Arrays.stream(keys).forEach(x -> {
                                if (ctx.getStore().containsKey(x))
                                    ret[0]++;
                            });
                            return RespType.ofLong(ret[0]);
                        },
                        "EXISTS key [key ...]",
                        "Determines whether one or more keys exist."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("EXPIRE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            long second = Long.parseLong(args.valueWithName("second"));
                            Long ret = expire(ctx, key, second * 1000, args.hasOption("NX")
                                    , args.hasOption("XX"), args.hasOption("GT"), args.hasOption("LT"));
                            return RespType.ofLong(ret);
                        },
                        "EXPIRE key seconds [NX | XX] [GT | LT]",
                        "Sets the expiration time of a key in seconds."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("second")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("GX"),
                                Command.Part.ofOptionNamedSimple("LX"))
                )
        );
        commands.put("EXPIREAT",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            long unitTimeSeconds = Long.parseLong(args.valueWithName("unix-time-seconds"));
                            Long ret = expireAt(ctx, key, unitTimeSeconds * 1000, args.hasOption("NX")
                                    , args.hasOption("XX"), args.hasOption("GT"), args.hasOption("LT"));
                            return RespType.ofLong(ret);
                        },
                        "EXPIREAT key unix-time-seconds [NX | XX] [GT | LT]",
                        "Sets the expiration time of a key to a Unix timestamp."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("unix-time-seconds")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("GX"),
                                Command.Part.ofOptionNamedSimple("LX"))
                )
        );
        commands.put("EXPIRETIME",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            return RespType.ofLong(expireTime(ctx, key ,false));
                        },
                        "EXPIRETIME key",
                        "Returns the expiration time of a key as a Unix timestamp."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("KEYS",
                new Command(
                        (ctx, args) -> {
                            String pattern = args.valueWithName("pattern");
                            PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
                            RespType[] keys = ctx.getStore().keySet()
                                    .stream()
                                    .filter(k -> pathMatcher.matches(Paths.get(k)))
                                    .map(RespType::ofBulkString)
                                    .toArray(RespType[]::new);
                            return RespType.ofArray(keys);
                            // FIXME: use a better glob match library
                        },
                        "KEYS pattern",
                        "Returns all key names that match a pattern."
                        , Command.Part.ofValue("pattern")
                )
        );
        commands.put("MIGRATE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "MIGRATE host port destination-db timeout [COPY] [REPLACE]  [AUTH password | AUTH2 username password] [KEYS key [key ...]]",
                        "Atomically transfers a key from one Redis instance to another."
                )
        );
        commands.put("MOVE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            if (!ctx.getStore().containsKey(key))
                                return RespType.ofLong(0L);

                            int idx = Integer.parseInt(args.valueWithName("db"));
                            if (idx < 0 || idx >= InMemorySharedStore.MAX_DB_SIZE)
                                throw new IllegalArgumentException("db index is out of range");

                            if (sharedStore.getDB(idx).containsKey(key))
                                return RespType.ofLong(0L);

                            sharedStore.getDB(idx).put(key, ctx.getStore().get(key));
                            ctx.getStore().remove(key);
                            return RespType.ofLong(1L);
                        },
                        "MOVE key db",
                        "Moves a key to another database."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("db")
                )
        );
        commands.put("OBJECT ENCODING",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "OBJECT ENCODING key",
                        "Returns the internal encoding of a Redis object."
                )
        );
        commands.put("OBJECT FREQ",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "OBJECT FREQ key",
                        "Returns the logarithmic access frequency counter of a Redis object."
                )
        );
        commands.put("OBJECT IDLETIME",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "OBJECT IDLETIME key",
                        "Returns the time since the last access to a Redis object."
                )
        );
        commands.put("OBJECT REFCOUNT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "OBJECT REFCOUNT key",
                        "Returns the reference count of a value of a key."
                )
        );
        commands.put("PERSIST",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            Long[] ret = new Long[1];
                            ret[0] = 0L;
                            ctx.getStore().compute(key, (k, oldV) -> {
                                if (oldV == null || oldV.getExpiredAt() == 0)
                                    return null;
                                oldV.setExpiredAt(null);
                                ret[0] = 1L;
                                return oldV;
                            });
                            return RespType.ofLong(ret[0]);
                        },
                        "PERSIST key",
                        "Removes the expiration time of a key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("PEXPIRE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            long milliseconds = Long.parseLong(args.valueWithName("milliseconds"));
                            Long ret = expire(ctx, key, milliseconds, args.hasOption("NX")
                                    , args.hasOption("XX"), args.hasOption("GT"), args.hasOption("LT"));
                            return RespType.ofLong(ret);
                        },
                        "PEXPIRE key milliseconds [NX | XX | GT | LT]",
                        "Sets the expiration time of a key in milliseconds."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("milliseconds")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("GX"),
                                Command.Part.ofOptionNamedSimple("LX"))
                )
        );
        commands.put("PEXPIREAT",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            long unitTimeMilliSeconds = Long.parseLong(args.valueWithName("unix-time-milliseconds"));
                            Long ret = expireAt(ctx, key, unitTimeMilliSeconds, args.hasOption("NX")
                                    , args.hasOption("XX"), args.hasOption("GT"), args.hasOption("LT"));
                            return RespType.ofLong(ret);
                        },
                        "PEXPIREAT key unix-time-milliseconds [NX | XX] [GT | LT]",
                        "Sets the expiration time of a key to a Unix milliseconds timestamp."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("unix-time-milliseconds")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("GX"),
                                Command.Part.ofOptionNamedSimple("LX"))
                )
        );
        commands.put("PEXPIRETIME",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            return RespType.ofLong(expireTime(ctx, key ,true));
                        },
                        "PEXPIRETIME key",
                        "Returns the expiration time of a key as a Unix milliseconds timestamp."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("PTTL",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            return RespType.ofLong(ttl(ctx, key, true));
                        },
                        "PTTL key",
                        "Returns the expiration time in milliseconds of a key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("RANDOMKEY",
                new Command(
                        (ctx, args) -> {
                            Set<String> ks = ctx.getStore().keySet();
                            int randomIndex = new Random().nextInt(ks.size());
                            Optional<String> randomElement = ks.stream().skip(randomIndex).findFirst();
                            return randomElement.map(RespType::ofBulkString).orElseGet(RespType::NullBulkString);
                        },
                        "RANDOMKEY",
                        "Returns a random key name from the database."
                )
        );
        commands.put("RENAME",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String newKey = args.valueWithName("newkey");
                            ctx.getStore().compute(key, (k, oldV) -> {
                                if (oldV == null)
                                    throw new IllegalArgumentException("no such key");
                                if (key.equals(newKey))
                                    return oldV;
                                else {
                                    ctx.getStore().put(newKey, oldV);
                                    return null;
                                }
                            });
                            return RespType.OK();
                        },
                        "RENAME key newkey",
                        "Renames a key and overwrites the destination."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("newkey")
                )
        );
        commands.put("RENAMENX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String newKey = args.valueWithName("newkey");
                            if (ctx.getStore().containsKey(newKey))
                                return RespType.ofLong(0L);

                            ctx.getStore().compute(key, (k, oldV) -> {
                                if (oldV == null)
                                    throw new IllegalArgumentException("no such key");
                                else {
                                    ctx.getStore().put(newKey, oldV);
                                    return null;
                                }
                            });
                            return RespType.ofLong(1L);
                        },
                        "RENAMENX key newkey",
                        "Renames a key only when the target key name doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("newkey")
                )
        );
        commands.put("RESTORE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "RESTORE key ttl serialized-value [REPLACE] [ABSTTL]  [IDLETIME seconds] [FREQ frequency]",
                        "Creates a key from the serialized representation of a value."
                )
        );
        commands.put("SCAN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]",
                        "Iterates over the key names in the database."
                )
        );
        commands.put("SORT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern  ...]] [ASC | DESC] [ALPHA] [STORE destination]",
                        "Sorts the elements in a list, a set, or a sorted set, optionally storing the result."
                )
        );
        commands.put("SORT_RO",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET  pattern ...]] [ASC | DESC] [ALPHA]",
                        "Returns the sorted elements of a list, a set, or a sorted set."
                )
        );
        commands.put("TOUCH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "TOUCH key [key ...]",
                        "Returns the number of existing keys out of those specified after updating the time they were last accessed."
                )
        );
        commands.put("TTL",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            return RespType.ofLong(ttl(ctx, key, false));
                        },
                        "TTL key",
                        "Returns the expiration time in seconds of a key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("TYPE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            return RespType.ofBulkString(ctx.getStore().containsKey(key)?
                                    ctx.getStore().get("key").getType().toString() : null);
                        },
                        "TYPE key",
                        "Determines the type of value stored at a key."
                )
        );
        commands.put("UNLINK",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "UNLINK key [key ...]",
                        "Asynchronously deletes one or more keys."
                )
        );
        commands.put("WAIT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "WAIT numreplicas timeout",
                        "Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed."
                )
        );
        commands.put("WAITAOF",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "WAITAOF numlocal numreplicas timeout",
                        "Blocks until all of the preceding write commands sent by the connection are written to the append-only file of the master and/or replicas."
                )
        );
    }

    private Long expireTime(Context ctx, String key, boolean versionP) {
        ValueWithTTL v = ctx.getStore().get(key);
        return v == null? -2L
                : v.getExpiredAt() == null? -1L
                : v.getExpiredAt() / (versionP? 1 : 1000);
    }

    private Long expireAt(Context ctx, String key, long unitTimeSeconds, boolean isNX, boolean isXX, boolean isGT, boolean isLT) {
        return handleExpire(ctx, key, unitTimeSeconds, isNX, isXX, isGT, isLT);
    }

    private Long expire(Context ctx, String key, Long milliseconds, boolean isNX, boolean isXX, boolean isGT, boolean isLT) {
        long unitTimeSeconds = System.currentTimeMillis() + milliseconds;
        return handleExpire(ctx, key, unitTimeSeconds, isNX, isXX, isGT, isLT);
    }

    private Long handleExpire(Context ctx, String key, long unitTimeSeconds, boolean isNX, boolean isXX, boolean isGT, boolean isLT) {
        Long[] ret = new Long[1];
        ctx.getStore().compute(key, (k, oldV) -> {
            if (oldV == null) {
                ret[0] = 0L;
                return null;
            }

            if ((isNX && oldV.getExpiredAt() == null)
                || (isXX && oldV.getExpiredAt() != null)
                || (isGT && oldV.getExpiredAt() != null && unitTimeSeconds <= Long.parseLong(oldV.getExpiredAt().toString()))
                || (isLT && (oldV.getExpiredAt() == null || unitTimeSeconds >= Long.parseLong(oldV.getExpiredAt().toString())))) {
                if (oldV.getExpiredAt() == null) {
                    oldV.setExpiredAt(unitTimeSeconds);
                    ret[0] = 1L;
                } else {
                    ret[0] = 0L;
                }
            }

            return oldV;
        });
        return ret[0];
    }

    private long ttl(Context ctx, String key, boolean versionP) {
        ValueWithTTL v = ctx.getStore().get(key);
        if (v == null) {
            return -2L;
        } else {
            return v.getExpiredAt() == null? -1 : (v.getExpiredAt() - System.currentTimeMillis())/(versionP? 1 : 1000);
        }
    }
}
