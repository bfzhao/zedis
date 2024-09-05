package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.my.handlers.SortedSetHandler.zsetInterCard;

@Component
public class SetHandler extends RedisCommandHandler {
    private final static ValueWithTTL.ValueType valueType = ValueWithTTL.ValueType.Set;

    @SuppressWarnings("unchecked")
    private <T> T handleSet(Context ctx, String key, Function<Set<String>, T> func) {
        T[] ret = (T[]) new Object[1];
        ret[0] = null;

        ctx.getStore().compute(key, (k, v) -> {
            if (v == null) {
                v = ValueWithTTL.ofSetValue();
            }

            assertValueType(valueType, v);
            Set<String> set = v.getValueAsSet();
            T t = func.apply(set);
            ret[0] = t;
            return set.size() == 0? null : v;
        });
        return ret[0];

    }

    private RespType setX(Context ctx, String[] keys, BiFunction<String[], Map<String, ValueWithTTL>, Set<String>> op) {
        Set<String> r = op.apply(keys, ctx.getStore());
        return RespType.ofArray(r.toArray(new String[0]));
    }

    private RespType setXStore(Context ctx, String[] keys, String dest, BiFunction<String[], Map<String, ValueWithTTL>, Set<String>> op) {
        Set<String> d = op.apply(keys, ctx.getStore());
        ctx.getStore().put(dest, ValueWithTTL.ofSetValue(d));
        return RespType.ofLong(d.size());
    }

    private static Set<String> diff(String[] keys, Map<String, ValueWithTTL> store) {
        ValueWithTTL v = store.get(keys[0]);
        if (v == null) {
            return ValueWithTTL.getSet();
        } else {
            Set<String> src = v.getValueAsSet();
            for (int i = 1; i < keys.length; i++) {
                ValueWithTTL t = store.get(keys[i]);
                if (t != null) {
                    src.removeAll(t.getValueAsSet());
                }
            }
            return src;
        }
    }

    private static Set<String> inter(String[] keys, Map<String, ValueWithTTL> store) {
        Set<String> minSet = Arrays.stream(keys).map(x -> {
            ValueWithTTL v = store.get(x);
            return v == null? ValueWithTTL.getSet() : v.getValueAsSet();
        }).min(Comparator.comparingInt(Set::size)).orElse(ValueWithTTL.getSet());

        if (minSet.size() == 0)
            return minSet;

        Set<String> ret = ValueWithTTL.getSet();
        ret.addAll(minSet);

        for (String k: keys) {
            ret.removeIf(v -> !store.get(k).getValueAsSet().contains(v));
            if (ret.size() == 0)
                break;
        }

        return ret;
    }

    private static Set<String> union(String[] keys, Map<String, ValueWithTTL> store) {
        return Arrays
                .stream(keys)
                .map(x -> {
                    ValueWithTTL v = store.get(x);
                    return v == null? ValueWithTTL.getSet() : v.getValueAsSet();
                })
                .flatMap(Set::stream)
                .map(Object::toString)
                .collect(Collectors.toSet());
    }

    private static RespType accessAndDelete(Context ctx, String key, String count) {
        if (!ctx.getStore().containsKey(key))
            return RespType.NullBulkString();

        List<String> ret = new ArrayList<>();
        if (count != null) {
            int c = Integer.parseInt(count);
            if (c < 0)
                throw new IllegalArgumentException("invalid count");

            if (c == 0)
                return RespType.NullBulkString();

            Integer[] n = new Integer[1];
            n[0] = c;
            ctx.getStore().computeIfPresent(key, (k, v) -> {
                Iterator<String> iterator = v.getValueAsSet().iterator();
                while (iterator.hasNext() && n[0]-- > 0) {
                    ret.add(iterator.next());
                    iterator.remove();
                }
                return v.getValueAsSet().size() == 0? null : v;
            });
        } else {
            ctx.getStore().computeIfPresent(key, (k, v) -> {
                Iterator<String> iterator = v.getValueAsSet().iterator();
                if (iterator.hasNext()) {
                    ret.add(iterator.next());
                    iterator.remove();
                }
                return v.getValueAsSet().size() == 0? null : v;
            });
        }
        return RespType.ofArray(ret.toArray(new String[0]));
    }

    SetHandler(InMemorySharedStore sharedStore) {
        super("Set", sharedStore);

        commands.put("SADD",
                new Command(
                        (ctx, args) -> {
                            int n = handleSet(ctx, args.valueWithName("key"), x -> {
                                int i = 0;
                                for (String m: args.valueListDefault()) {
                                    if (!x.contains(m)) {
                                        x.add(m);
                                        i++;
                                    }
                                }
                                return i;
                            });
                            return RespType.ofLong(n);
                        },
                        "SADD key member [member ...]",
                        "Adds one or more members to a set. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("member")
                )
        );
        commands.put("SCARD",
                new Command(
                        (ctx, args) -> {
                            int ret = handleSet(ctx, args.valueWithName("key"), Set::size);
                            return RespType.ofLong(ret);
                        },
                        "SCARD key",
                        "Returns the number of members in a set."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("SDIFF",
                new Command(
                        (ctx, args) -> setX(ctx, args.valueListDefault(), SetHandler::diff),
                        "SDIFF key [key ...]",
                        "Returns the difference of multiple sets."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("SDIFFSTORE",
                new Command(
                        (ctx, args) -> setXStore(ctx, args.valueListDefault(), args.valueWithName("destination"), SetHandler::diff),
                        "SDIFFSTORE destination key [key ...]",
                        "Stores the difference of multiple sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("SINTER",
                new Command(
                        (ctx, args) -> setX(ctx, args.valueListDefault(), SetHandler::inter),
                        "SINTER key [key ...]",
                        "Returns the intersect of multiple sets."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("SINTERCARD",
                new Command(
                        (ctx, args) -> {
                            long ret = zsetInterCard(ctx, args.valueListDefault(), args.valueWithName("LIMIT"));
                            return RespType.ofLong(ret);
                        },
                        "SINTERCARD numkeys key [key ...] [LIMIT limit]",
                        "Returns the number of members of the intersect of multiple sets."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "limit")
                )
        );
        commands.put("SINTERSTORE",
                new Command(
                        (ctx, args) -> setXStore(ctx, args.valueListDefault(), args.valueWithName("destination"), SetHandler::inter),
                        "SINTERSTORE destination key [key ...]",
                        "Stores the intersect of multiple sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("SISMEMBER",
                new Command(
                        (ctx, args) -> {
                            boolean is = handleSet(ctx, args.valueWithName("key"), x -> x.contains(args.valueWithName("member")));
                            return RespType.ofLong(is? 1 : 0);
                        },
                        "SISMEMBER key member",
                        "Determines whether a member belongs to a set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("member")
                )
        );
        commands.put("SMEMBERS",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleSet(ctx, args.valueWithName("key"), x -> x.toArray(new String[0]));
                            return RespType.ofArray(ret);
                        },
                        "SMEMBERS key",
                        "Returns all members of a set."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("SMISMEMBER",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String[] members = args.valueListDefault();

                            Integer[] ret = new Integer[members.length];
                            ctx.getStore().computeIfPresent(key, (k, v) -> {
                                for (int i = 0; i < members.length; i++) {
                                    ret[i] = v.getValueAsSet().contains(members[i])? 1 : 0;
                                }
                                return v;
                            });
                            return RespType.ofArray(ret);
                        },
                        "SMISMEMBER key member [member ...]",
                        "Determines whether multiple members belong to a set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("member")
                )
        );
        commands.put("SMOVE",
                new Command(
                        (ctx, args) -> {
                            int ret = handleSet(ctx, args.valueWithName("source"), s -> {
                                String member = args.valueWithName("member");
                                if (s.contains(member)) {
                                    boolean done = handleSet(ctx, args.valueWithName("destination"), d -> d.add(member));
                                    if (done) {
                                        s.remove(member);
                                        return 1;
                                    }
                                }

                                return 0;
                            });
                            return RespType.ofLong(ret);
                        },
                        "SMOVE source destination member",
                        "Moves a member from one set to another."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofValue("member")
                )
        );
        commands.put("SPOP",
                new Command(
                        (ctx, args) -> accessAndDelete(ctx, args.valueWithName("key"), args.optionValueAnonymous()),
                        "SPOP key [count]",
                        "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("SRANDMEMBER",
                new Command(
                        (ctx, args) -> handleSet(ctx, args.valueWithName("key"),
                                x -> handleRandomSelect(x,args.optionValueAnonymous(), false, null)),
                        "SRANDMEMBER key [count]",
                        "Get one or multiple random members from a set"
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("SREM",
                new Command(
                        (ctx, args) -> {
                            int n = handleSet(ctx, args.valueWithName("key"), x -> {
                                int c = 0;
                                String[] members = args.valueListDefault();
                                for (String m: members) {
                                    if (x.contains(m)) {
                                        c++;
                                        x.remove(m);
                                    }
                                }
                                return c;
                            });
                            return RespType.ofLong(n);
                        },
                        "SREM key member [member ...]",
                        "Removes one or more members from a set. Deletes the set if the last member was removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("member")
                )
        );
        commands.put("SSCAN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "SSCAN key cursor [MATCH pattern] [COUNT count]",
                        "Iterates over members of a set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("cursor")
                        , Command.Part.ofOptionNamedSimple("MATCH", "pattern")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("SUNION",
                new Command(
                        (ctx, args) -> setX(ctx, args.valueListDefault(), SetHandler::union),
                        "SUNION key [key ...]",
                        "Returns the union of multiple sets."
                        , Command.Part.ofListValue("key")
                )
        );
        commands.put("SUNIONSTORE",
                new Command(
                        (ctx, args) -> setXStore(ctx, args.valueListDefault(), args.valueWithName("destination"), SetHandler::union),
                        "SUNIONSTORE destination key [key ...]",
                        "Stores the union of multiple sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofListValue("key")
                )
        );
    }
}
