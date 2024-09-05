package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.util.stream.Stream;

@Component
public class StringHandler extends RedisCommandHandler {
    StringHandler(InMemorySharedStore inMemorySharedStore) {
        super("String", inMemorySharedStore);

        commands.put("APPEND",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String value = args.valueWithName("value");
                            Integer[] v = new Integer[1];
                            ctx.getStore().compute(key, (k, oldV) -> {
                                assertValueType(oldV);
                                if (oldV == null) {
                                    v[0] = value.length();
                                    return ValueWithTTL.ofString(value);
                                } else {
                                    String nv = oldV.getValue().toString() + value;
                                    v[0] = nv.length();
                                    return ValueWithTTL.ofString(nv, oldV);
                                }
                            });
                            return RespType.ofLong(v[0]);
                        },
                        "APPEND key value",
                        "Appends a string to the value of a key. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("value")
                )
        );
        commands.put("DECR",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            Long[] v = new Long[1];
                            ctx.getStore().compute(key, (k, oldV) -> {
                                assertValueType(oldV);
                                return decrBy(v, oldV, 1);
                            });
                            return RespType.ofLong(v[0]);
                        },
                        "DECR key",
                        "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("DECRBY",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String decrement = args.valueWithName("decrement");
                            Long[] v = new Long[1];
                            ctx.getStore().compute(key, (k, oldV) -> {
                                assertValueType(oldV);
                                return decrBy(v, oldV, Integer.parseInt(decrement));
                            });
                            return RespType.ofLong(v[0]);
                        },
                        "DECRBY key decrement",
                        "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("decrement")
                )
        );
        commands.put("GET",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            ValueWithTTL v = ctx.getStore().get(key);
                            assertValueType(v);
                            return v == null? RespType.NullBulkString() : RespType.ofBulkString(v.getValue().toString());
                        },
                        "GET key",
                        "Returns the string value of a key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("GETDEL",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            ValueWithTTL v = ctx.getStore().get(key);
                            assertValueType(v);
                            if (v != null)
                                ctx.getStore().remove(key);
                            return v == null? RespType.NullBulkString() : RespType.ofBulkString(v.getValue().toString());
                        },
                        "GETDEL key",
                        "Returns the string value of a key after deleting the key."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("GETEX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            boolean isPersist = args.hasOption("PERSIST");
                            Long expiredAt;
                            if (args.hasOption("EX"))
                                expiredAt = System.currentTimeMillis() + Integer.parseInt(args.optionWithName("EX")[0]) * 1000;
                            else if (args.hasOption("PX"))
                                expiredAt = System.currentTimeMillis() + Integer.parseInt(args.optionWithName("PX")[0]);
                            else if (args.hasOption("EXAT"))
                                expiredAt = Long.parseLong(args.optionWithName("EXAT")[0]) * 1000;
                            else if (args.hasOption("PXAT"))
                                expiredAt = Long.parseLong(args.optionWithName("PXAT")[0]);
                            else if (args.hasOption("KEEPTTL"))
                                expiredAt = Long.parseLong(args.optionWithName("PXAT")[0]);
                            else
                                expiredAt = null;

                            ValueWithTTL[] last = new ValueWithTTL[1];
                            ctx.getStore().compute(key, (k, v) -> {
                                assertValueType(v);
                                last[0] = v;
                                if (v != null) {
                                    v.setExpiredAt(isPersist? null : expiredAt);
                                }
                                return v;
                            });

                            return last[0] == null? RespType.NullBulkString() : RespType.ofBulkString(last[0].getValue().toString());
                        },
                        "GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds |  PXAT unix-time-milliseconds | PERSIST]",
                        "Returns the string value of a key after setting its expiration time."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("EX", "seconds"),
                                Command.Part.ofOptionNamedSimple("PX", "milliseconds"),
                                Command.Part.ofOptionNamedSimple("EXAT", "unix-time-seconds"),
                                Command.Part.ofOptionNamedSimple("PXAT", "unix-time-milliseconds"),
                                Command.Part.ofOptionNamedSimple("PERSIST"))
                )
        );
        // REDIS GETRANGE cannot handle unicode correctly
        commands.put("GETRANGE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            int start = Integer.parseInt(args.valueWithName("start"));
                            int end = Integer.parseInt(args.valueWithName("end"));
                            ValueWithTTL v = ctx.getStore().get(key);
                            assertValueType(v);
                            if (v == null) {
                                return RespType.ofBulkString("");
                            } else {
                                String vv = v.getValue().toString();
                                if (start < 0)
                                    start = vv.length() + start;
                                if (end < 0)
                                    end = vv.length() + end;

                                if (start >= vv.length() || end < start)
                                    return RespType.ofBulkString("");

                                if (end >= vv.length())
                                    end = vv.length() - 1;

                                return RespType.ofBulkString(vv.substring(start, end+1));
                            }
                        },
                        "GETRANGE key start end",
                        "Returns a substring of the string stored at a key."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("end")
                )
        );
        commands.put("GETSET",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String value = args.valueWithName("value");

                            String[] ret = new String[1];
                            ctx.getStore().compute(key, (k, oldV) ->  {
                                assertValueType(oldV);
                                ret[0] = oldV == null? null : oldV.toString();
                                return new ValueWithTTL(ValueWithTTL.ValueType.String, value);
                            });

                            return ret[0] == null? RespType.NullBulkString() : RespType.ofBulkString(ret[0]);
                        },
                        "GETSET key value",
                        "Returns the previous string value of a key after setting it to a new value."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("value")
                )
        );
        commands.put("INCR",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            Long[] v = new Long[1];
                            ctx.getStore().compute(key, (k, oldV) -> {
                                assertValueType(oldV);
                                return decrBy(v, oldV, -1);
                            });
                            return RespType.ofLong(v[0]);
                        },
                        "INCR key",
                        "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("INCRBY",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String increment = args.valueWithName("increment");
                            Long[] v = new Long[1];
                            ctx.getStore().compute(key, (k, oldV) -> {
                                assertValueType(oldV);
                                return decrBy(v, oldV, -Integer.parseInt(increment));
                            });
                            return RespType.ofLong(v[0]);
                        },
                        "INCRBY key increment",
                        "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("increment")
                )
        );
        commands.put("INCRBYFLOAT",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "INCRBYFLOAT key increment",
                        "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("increment")
                )
        );
        commands.put("LCS",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min-match-len] [WITHMATCHLEN]",
                        "Finds the longest common substring."
                        , Command.Part.ofValue("key1")
                        , Command.Part.ofValue("key2")
                        , Command.Part.ofOptionNamedSimple("LEN")
                        , Command.Part.ofOptionNamedSimple("IDX")
                        , Command.Part.ofOptionNamedSimple("MINMATCHLEN", "min-match-len")
                        , Command.Part.ofOptionNamedSimple("WITHMATCHLEN")
                )
        );
        commands.put("MGET",
                new Command(
                        (ctx, args) -> {
                            String[] keys = args.valueListDefault();
                            synchronized (ctx.getStore()) {
                                RespType[] ret = Stream.of(keys)
                                        .map(ctx.getStore()::get)
                                        .map(x -> x == null || x.getType() != ValueWithTTL.ValueType.String?
                                                RespType.NullBulkString() : RespType.ofString(x.getValue().toString()))
                                        .toArray(RespType[]::new);
                                return RespType.ofArray(ret);
                            }
                        },
                        "MGET key [key ...]",
                        "Atomically returns the string values of one or more keys."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("MSET",
                new Command(
                        (ctx, args) -> {
                            String[] kvs = args.valueListDefault();
                            assert (kvs.length %2 == 0);
                            synchronized (ctx.getStore()) {
                                for (int i = 0; i < kvs.length/2; ++i) {
                                    ctx.getStore().put(kvs[i * 2], new ValueWithTTL(ValueWithTTL.ValueType.String, kvs[i * 2 + 1]));
                                }
                            }
                            return RespType.OK();
                        },
                        "MSET key value [key value ...]",
                        "Atomically creates or modifies the string values of one or more keys."
                        , Command.Part.ofListValue("key", "value")
                )
        );
        commands.put("MSETNX",
                new Command(
                        (ctx, args) -> {
                            String[] kvs = args.valueListDefault();
                            assert (kvs.length %2 == 0);
                            synchronized (ctx.getStore()) {
                                for (int i = 0; i < kvs.length/2; ++i) {
                                    if (ctx.getStore().containsKey(kvs[i * 2])) {
                                        return RespType.ofLong(0L);
                                    }
                                }

                                for (int i = 0; i < kvs.length/2; ++i) {
                                    ctx.getStore().put(kvs[i * 2], new ValueWithTTL(ValueWithTTL.ValueType.String, kvs[i * 2 + 1]));
                                }
                                return RespType.ofLong(1L);
                            }
                        },
                        "MSETNX key value [key value ...]",
                        "Atomically modifies the string values of one or more keys only when all keys don't exist."
                        , Command.Part.ofListValue("key", "value")
                )
        );
        commands.put("PSETEX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            int milliseconds = Integer.parseInt(args.valueWithName("milliseconds"));
                            String value = args.valueWithName("value");
                            if (milliseconds <= 0) {
                                return RespType.ofError("invalid expire time");
                            } else {
                                ctx.getStore().put(key, new ValueWithTTL(ValueWithTTL.ValueType.String, value, System.currentTimeMillis() + milliseconds));
                                return RespType.ofError("not implemented");
                            }
                        },
                        "PSETEX key milliseconds value",
                        "Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("milliseconds")
                        , Command.Part.ofValue("value")
                )
        );
        commands.put("SET",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String value = args.valueWithName("value");
                            boolean isGet = args.hasOption("GET");
                            boolean isNx = args.hasOption("NX");
                            boolean isXx = args.hasOption("XX");
                            boolean isKeepTtl = args.hasOption("KEEPTTL");
                            Long expiredAt;
                            if (args.hasOption("EX"))
                                expiredAt = System.currentTimeMillis() + Integer.parseInt(args.optionWithName("EX")[0]) * 1000;
                            else if (args.hasOption("PX"))
                                expiredAt = System.currentTimeMillis() + Integer.parseInt(args.optionWithName("PX")[0]);
                            else if (args.hasOption("EXAT"))
                                expiredAt = Long.parseLong(args.optionWithName("EXAT")[0]) * 1000;
                            else if (args.hasOption("PXAT"))
                                expiredAt = Long.parseLong(args.optionWithName("PXAT")[0]);
                            else if (args.hasOption("KEEPTTL"))
                                expiredAt = Long.parseLong(args.optionWithName("PXAT")[0]);
                            else
                                expiredAt = null;

                            ValueWithTTL[] last = new ValueWithTTL[1];
                            Boolean[] result = new Boolean[1];
                            ctx.getStore().compute(key, (k, v) -> {
                                last[0] = v;
                                if (v == null) {
                                    result[0] = isNx || !isXx;
                                    return result[0]? new ValueWithTTL(ValueWithTTL.ValueType.String, value, expiredAt) : null;
                                } else {
                                    result[0] = isXx || !isNx;
                                    return result[0]? new ValueWithTTL(ValueWithTTL.ValueType.String, value, isKeepTtl? v.getExpiredAt() : expiredAt) : v;
                                }
                            });

                            if (isGet) {
                                return last[0] == null? RespType.NullBulkString() : RespType.ofBulkString(last[0].getValue().toString());
                            } else {
                                return result[0]? RespType.OK() : RespType.NullBulkString();
                            }
                        },
                        "SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]",
                        "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("value")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionNamedSimple("GET")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("EX", "seconds"),
                                Command.Part.ofOptionNamedSimple("PX", "milliseconds"),
                                Command.Part.ofOptionNamedSimple("EXAT", "unix-time-seconds"),
                                Command.Part.ofOptionNamedSimple("PXAT", "unix-time-milliseconds"),
                                Command.Part.ofOptionNamedSimple("KEEPTTL"))
                )
        );
        commands.put("SETEX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            int seconds = Integer.parseInt(args.valueWithName("seconds"));
                            String value = args.valueWithName("value");
                            if (seconds <= 0) {
                                return RespType.ofError("invalid expire time");
                            } else {
                                ctx.getStore().put(key, new ValueWithTTL(ValueWithTTL.ValueType.String, value, System.currentTimeMillis() + seconds * 1000));
                                return RespType.ofError("not implemented");
                            }
                        },
                        "SETEX key seconds value",
                        "Sets the string value and expiration time of a key. Creates the key if it doesn't exist."
                )
        );
        commands.put("SETNX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String value = args.valueWithName("value");
                            Object o = ctx.getStore().putIfAbsent(key, new ValueWithTTL(ValueWithTTL.ValueType.String, value));
                            return RespType.ofLong(o == null? 1L : 0L);
                        },
                        "SETNX key value",
                        "Set the string value of a key only when the key doesn't exist."
                )
        );
        commands.put("SETRANGE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            int offset = Integer.parseInt(args.valueWithName("offset"));
                            String value = args.valueWithName("value");
                            if (offset < 0) {
                                return RespType.ofError("offset is out of range");
                            }

                            Long[] len = new Long[1];
                            ctx.getStore().compute(key, (k, v) -> {
                                assertValueType(v);
                                String ret;
                                if (v == null) {
                                    ret = new String(new char[offset]) + value;
                                } else {
                                    String ov = v.toString();
                                    if (offset > ov.length()) {
                                        ret = ov + new String(new char[offset - ov.length()]) + value;
                                    } else {
                                        ret = ov.substring(0, offset) + value +
                                                (ov.length() > offset + value.length() ? ov.substring(offset + value.length()) : "");
                                    }
                                }
                                len[0] = (long) ret.length();
                                return new ValueWithTTL(ValueWithTTL.ValueType.String, ret, v == null? null : v.getExpiredAt());
                            });
                            return RespType.ofLong(len[0]);
                        },
                        "SETRANGE key offset value",
                        "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("offset")
                        , Command.Part.ofValue("value")
                )
        );
        commands.put("STRLEN",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL v = ctx.getStore().get(args.valueWithName("key"));
                            assertValueType(v);
                            return v == null? RespType.ofLong(0L) : RespType.ofLong(v.getValue().toString().length());
                        },
                        "STRLEN key",
                        "Returns the length of a string value."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("SUBSTR",
                new Command(
                        commands.get("GETRANGE").getFunc(),
                        "SUBSTR key start end",
                        "Returns a substring from a string value."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("end")
                )
        );
    }

    private void assertValueType(ValueWithTTL value) {
        assertValueType(ValueWithTTL.ValueType.String, value);
    }

    private ValueWithTTL decrBy(Long[] v, ValueWithTTL oldV, int by) {
        if (oldV == null)
            oldV = new ValueWithTTL(ValueWithTTL.ValueType.String, 0);

        if (oldV.getValue() instanceof Long) {
            long nv = (Long) oldV.getValue() - by;
            v[0] = nv;
            return new ValueWithTTL(nv, oldV);
        } else if (oldV.getValue() instanceof String) {
            try {
                long nv = Long.parseLong((String) oldV.getValue()) - by;
                v[0] = nv;
                return new ValueWithTTL(nv, oldV);
            } catch (Exception e) {
                throw new IllegalArgumentException("not an number");
            }
        } else {
            throw new IllegalArgumentException("not an number");
        }
    }
}
