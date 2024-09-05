package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;

@Component
public class HashHandler extends RedisCommandHandler {
    private final ValueWithTTL.ValueType valueType = ValueWithTTL.ValueType.Hash;

    @SuppressWarnings("unchecked")
    private <T> T handleHash(Context ctx, String key, Function<Map<String, String>, T> func) {
        T[] ret = (T[]) new Object[1];
        ret[0] = null;

        ctx.getStore().compute(key, (k, v) -> {
            if (v == null) {
                v = ValueWithTTL.ofHashValue();
            }

            assertValueType(valueType, v);
            Map<String, String> map = v.getValueAsHash();
            T t = func.apply(map);
            ret[0] = t;
            return map.size() == 0? null : v;
        });
        return ret[0];
    }

    HashHandler(InMemorySharedStore sharedStore) {
        super("Hash", sharedStore);

        commands.put("HDEL",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String[] fields = args.valueListDefault();
                            Integer n = handleHash(ctx, key, x -> {
                                int r = 0;
                                for (String f: fields) {
                                    if (x.remove(f) != null)
                                        r++;
                                }
                                return r;
                            });
                            return RespType.ofLong(n == null? 0 : n);
                        },
                        "HDEL key field [field ...]",
                        "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("field")
                )
        );
        commands.put("HEXISTS",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String field = args.valueWithName("field");
                            Integer n = handleHash(ctx, key, x -> x.containsKey(field)? 1 : 0);
                            return RespType.ofLong(n == null? 0 : n);
                        },
                        "HEXISTS key field",
                        "Determines whether a field exists in a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("field")
                )
        );
        commands.put("HGET",
                new Command(
                        (ctx, args) -> {
                            String ret = handleHash(ctx, args.valueWithName("key"), x -> x.get(args.valueWithName("field")));
                            return ret == null? RespType.emptyArray() : RespType.ofBulkString(ret);
                        },
                        "HGET key field",
                        "Returns the value of a field in a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("field")
                )
        );
        commands.put("HGETALL",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleHash(ctx, args.valueWithName("key"), x -> {
                                List<String> r = new ArrayList<>();
                                for (String k: x.keySet()) {
                                    r.add(k);
                                    r.add(x.get(k));
                                }
                                return r.toArray(new String[0]);
                            });
                            return RespType.ofArray(ret);
                        },
                        "HGETALL key",
                        "Returns all fields and values in a hash."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("HINCRBY",
                new Command(
                        (ctx, args) -> {
                            long ret = handleHash(ctx, args.valueWithName("key"), x -> {
                                long v = 0L;
                                String k = args.valueWithName("field");
                                if (x.containsKey(k)) {
                                    v = Long.parseLong(x.get(k));
                                }
                                v += Long.parseLong(args.valueWithName("increment"));
                                x.put(k, Long.toString(v));
                                return v;
                            });
                            return RespType.ofLong(ret);
                        },
                        "HINCRBY key field increment",
                        "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("field")
                        , Command.Part.ofValue("increment")
                )
        );
        commands.put("HINCRBYFLOAT",
                new Command(
                        (ctx, args) -> {
                            double ret = handleHash(ctx, args.valueWithName("key"), x -> {
                                double v = 0;
                                String k = args.valueWithName("field");
                                if (x.containsKey(k)) {
                                    v = Double.parseDouble(x.get(k));
                                }
                                v += Double.parseDouble(args.valueWithName("increment"));
                                x.put(k, Double.toString(v));
                                return v;
                            });
                            return RespType.ofBulkString(Double.toString(ret));
                        },
                        "HINCRBYFLOAT key field increment",
                        "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist."
                )
        );
        commands.put("HKEYS",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleHash(ctx, args.valueWithName("key"), x -> x.keySet().toArray(new String[0]));
                            return RespType.ofArray(ret);
                        },
                        "HKEYS key",
                        "Returns all fields in a hash."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("HLEN",
                new Command(
                        (ctx, args) -> {
                            int len = handleHash(ctx, args.valueWithName("key"), x -> x.keySet().size());
                            return RespType.ofLong(len);
                        },
                        "HLEN key",
                        "Returns the number of fields in a hash."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("HMGET",
                new Command(
                        (ctx, args) -> {
                            String[] fields = args.valueListDefault();
                            String[] v = handleHash(ctx, args.valueWithName("key"), x -> {
                                String[] nv = new String[fields.length];
                                for (int i = 0; i < fields.length; i++)
                                    nv[i] = x.get(fields[i]);
                                return nv;
                            });
                            return RespType.ofArray(v);
                        },
                        "HMGET key field [field ...]",
                        "Returns the values of all fields in a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("field")
                )
        );
        commands.put("HMSET",
                new Command(
                        (ctx, args) -> {
                            String[] kvs = args.valueListDefault();
                            handleHash(ctx, args.valueWithName("key"), x -> {
                                for (int i = 0; i < kvs.length / 2; i++)
                                    x.put(kvs[i * 2], kvs[i * 2 + 1]);
                                return 0;
                            });
                            return RespType.OK();
                        },
                        "HMSET key field value [field value ...]",
                        "Sets the values of multiple fields."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("field", "value")
                )
        );
        commands.put("HRANDFIELD",
                new Command(
                        (ctx, args) -> handleHash(ctx, args.valueWithName("key"),
                                x -> handleRandomSelect(x.keySet(), args.optionValueAnonymous(),
                                    args.optionValueTerms().contains("WITHVALUES"), x::get)),
                        "HRANDFIELD key [count [WITHVALUES]]",
                        "Returns one or more random fields from a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymousWithTerm("count", "WITHVALUES")
                )
        );
        commands.put("HSCAN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "HSCAN key cursor [MATCH pattern] [COUNT count]",
                        "Iterates over fields and values of a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("cursor")
                        , Command.Part.ofOptionNamedSimple("MATCH", "pattern")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("HSET",
                new Command(
                        (ctx, args) -> {
                            String[] kvs = args.valueListDefault();
                            int n = handleHash(ctx, args.valueWithName("key"), x -> {
                                for (int i = 0; i < kvs.length / 2; i++)
                                    x.put(kvs[i * 2], kvs[i * 2 + 1]);
                                return kvs.length / 2;
                            });
                            return RespType.ofLong(n);
                        },
                        "HSET key field value [field value ...]",
                        "Creates or modifies the value of a field in a hash."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("field", "value")
                )
        );
        commands.put("HSETNX",
                new Command(
                        (ctx, args) -> {
                            int n = handleHash(ctx, args.valueWithName("key"), x -> {
                                if (x.containsKey(args.valueWithName("field")))
                                    return 0;

                                x.put(args.valueWithName("field"), args.valueWithName("value"));
                                return 1;
                            });
                            return RespType.ofLong(n);
                        },
                        "HSETNX key field value",
                        "Sets the value of a field in a hash only when the field doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("field")
                        , Command.Part.ofValue("value")
                )
        );
        commands.put("HSTRLEN",
                new Command(
                        (ctx, args) -> {
                            Integer ret = handleHash(ctx, args.valueWithName("key"), x -> {
                                if (x.containsKey(args.valueWithName("field")))
                                    return x.get(args.valueWithName("field")).length();
                                else
                                    return 0;
                            });
                            return RespType.ofLong(ret);
                        },
                        "HSTRLEN key field",
                        "Returns the length of the value of a field."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("field")
                )
        );
        commands.put("HVALS",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleHash(ctx, args.valueWithName("key"), x -> x.keySet().stream().map(x::get).toArray(String[]::new));
                            return RespType.ofArray(ret);
                        },
                        "HVALS key",
                        "Returns all values in a hash."
                        , Command.Part.ofValue("key")
                )
        );
    }
}
