package org.my.handlers;

import org.my.Command;
import org.my.Context;
import org.my.ValueWithTTL;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SortedSetHandler extends RedisCommandHandler {
    private final ValueWithTTL.ValueType valueType = ValueWithTTL.ValueType.SortedSet;

    @SuppressWarnings("unchecked")
    private <T> T handleSortSet(Context ctx, String key, Function<ValueWithTTL.ZSet, T> func) {
        T[] ret = (T[]) new Object[1];
        ret[0] = null;

        ctx.getStore().compute(key, (k, v) -> {
            if (v == null) {
                v = ValueWithTTL.ofSortedSetValue();
            }

            assertValueType(valueType, v);
            ValueWithTTL.ZSet set = v.getValueAsSortedSet();
            T t = func.apply(set);
            ret[0] = t;
            return set.size() == 0? null : v;
        });
        return ret[0];

    }

    SortedSet<ValueWithTTL.ZSet.Item> rankByScore(ValueWithTTL.ZSet set, String min, String max) {
        boolean minInclusive = !min.startsWith("(");
        boolean maxInclusive = !max.startsWith("(");
        double minV = Double.parseDouble(min.replaceFirst("^\\(", ""));
        double maxV = Double.parseDouble(max.replaceFirst("^\\(", ""));
        return set.asSet().subSet(ValueWithTTL.ofItem(minV), minInclusive,
                ValueWithTTL.ofItem(maxV), maxInclusive);
    }

    ValueWithTTL.ZSet zsetDiff(Context ctx, String[] keys) {
        ValueWithTTL.ZSet src = new ValueWithTTL.ZSet();
        ctx.getStore().get(keys[0]).getValueAsSortedSet().asSet().forEach(src::add);
        for (int i = 1; i < keys.length; i++) {
            ctx.getStore().get(keys[i]).getValueAsSortedSet().asSet().forEach(src::remove);
        }
        return src;
    }

    public static long zsetInterCard(Context ctx, String[] keys, String limit) {
        Set<String> minSet = Arrays.stream(keys).map(x -> {
            ValueWithTTL v = ctx.getStore().get(x);
            return v == null? ValueWithTTL.getSet() : v.getValueAsSet();
        }).min(Comparator.comparingInt(Set::size)).orElse(ValueWithTTL.getSet());

        if (minSet.size() == 0)
            return 0;

        int card = 0;
        int cardCount = Integer.parseInt(limit);

        for (String k: minSet) {
            boolean inter = Arrays.stream(keys)
                    .map(v -> ctx.getStore().get(k).getValueAsSortedSet().containsKey(v))
                    .reduce(Boolean::logicalAnd).orElse(false);

            if (inter) {
                card++;
                if (card >= cardCount)
                    break;
            }
        }

        return card;
    }

    ValueWithTTL.ZSet zsetInter(Context ctx, String[] keys, String[] weights, String aggregate) {
        if (keys.length != weights.length)
            throw new IllegalArgumentException("syntax error");

        Set<String> minSet = Arrays.stream(keys).map(x -> {
            ValueWithTTL v = ctx.getStore().get(x);
            return v == null? ValueWithTTL.getSet() : v.getValueAsSet();
        }).min(Comparator.comparingInt(Set::size)).orElse(ValueWithTTL.getSet());

        ValueWithTTL.ZSet src = new ValueWithTTL.ZSet();
        if (minSet.size() == 0)
            return src;

        Double[] factor = Arrays.stream(weights).map(Double::parseDouble).toArray(Double[]::new);
        Function<List<Double>, Double> func = AGGREGATE_FUNCS.get(aggregate);
        for (String k: minSet) {
            boolean inter = Arrays.stream(keys)
                    .map(v -> ctx.getStore().get(k).getValueAsSortedSet().containsKey(v))
                    .reduce(Boolean::logicalAnd).orElse(false);

            if (inter) {
                double v = aggregateSetMember(ctx, keys, factor, k, func);
                src.add(ValueWithTTL.ofItem(k, v));
            }
        }

        return src;
    }

    private double aggregateSetMember(Context ctx, String[] keys, Double[] factor, String member, Function<List<Double>, Double> func) {
        List<Double> pool = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; ++i) {
            if (ctx.getStore().get(keys[i]).getValueAsSortedSet().containsKey(member)) {
                pool.add(ctx.getStore().get(keys[i]).getValueAsSortedSet().getScore(member) * factor[i]);
            }
        }

        return func.apply(pool);
    }

    private static final Map<String, Function<List<Double>, Double>> AGGREGATE_FUNCS = new HashMap<>();
    static {
        AGGREGATE_FUNCS.put("MIN", x -> x.stream().min(Double::compare).orElse(0d));
        AGGREGATE_FUNCS.put("MAX", x -> x.stream().max(Double::compare).orElse(0d));
        AGGREGATE_FUNCS.put("SUM", x -> x.stream().mapToDouble(Double::doubleValue).sum());
    }

    ValueWithTTL.ZSet zsetUnion(Context ctx, String[] keys, String[] weights, String aggregate) {
        if (keys.length != weights.length)
            throw new IllegalArgumentException("syntax error");

        Set<String> keySet = Arrays
                .stream(keys)
                .map(x -> {
                    ValueWithTTL.ZSet v = ctx.getStore().get(x).getValueAsSortedSet();
                    return v == null? ValueWithTTL.getSortedSet() : v;
                })
                .map(ValueWithTTL.ZSet::asSet)
                .flatMap(Set::stream)
                .map(Object::toString)
                .collect(Collectors.toSet());

        Double[] factor = Arrays.stream(weights).map(Double::parseDouble).toArray(Double[]::new);
        Function<List<Double>, Double> func = AGGREGATE_FUNCS.get(aggregate);
        ValueWithTTL.ZSet src = new ValueWithTTL.ZSet();
        for (String x: keySet) {
            double v = aggregateSetMember(ctx, keys, factor, x, func);
            src.add(ValueWithTTL.ofItem(x, v));
        }

        return src;
    }

    Set<ValueWithTTL.ZSet.Item> zsetRange(Context ctx, String key, String min, String max, boolean byScoreOrLex, boolean isRev, String offset, String count) {
        // FIXME: this does not work as REDIS
        String startStr = isRev? max : min;
        String endStr = isRev? min : max;

        if (byScoreOrLex) {
            double start;
            boolean startInclusive = !startStr.startsWith("(");
            if (startStr.equals("-inf")) {
                start = Double.MIN_VALUE;
            } else {
                if (!startInclusive) {
                    startStr = startStr.substring(1);
                }
                start = Double.parseDouble(startStr);
            }

            double end;
            boolean endInclusive = !endStr.startsWith("(");
            if (endStr.equals("+inf")) {
                end = Double.MAX_VALUE;
            } else {
                if (!endInclusive) {
                    endStr = endStr.substring(1);
                }
                end = Double.parseDouble(endStr);
            }

            if (start > end) {
                throw new IllegalArgumentException("invalid min/max value");
            }

            SortedSet<ValueWithTTL.ZSet.Item> r = handleSortSet(ctx, key,
                    x -> x.asSet().subSet(ValueWithTTL.ofItem(start), startInclusive, ValueWithTTL.ofItem(end), endInclusive));
            if (offset != null) {
                int o = Integer.parseInt(offset);
                if (o < 0) {
                    return new ValueWithTTL.ZSet().asSet();
                }
                int c = Integer.parseInt(count);
                if (c < 0) {
                    return r.stream().skip(o).collect(Collectors.toSet());
                } else {
                    return r.stream().skip(o).limit(c).collect(Collectors.toSet());
                }
            } else {
                return r;
            }
        } else {
            boolean startInclusive;
            if (startStr.startsWith("["))
                startInclusive = true;
            else if (startStr.startsWith("("))
                startInclusive = false;
            else {
                startInclusive = false;
                if (!startStr.equals("-"))
                    throw new IllegalArgumentException("syntax error");
            }

            boolean endInclusive;
            if (endStr.startsWith("["))
                endInclusive = true;
            else if (endStr.startsWith("("))
                endInclusive = false;
            else {
                endInclusive = false;
                if (!endStr.equals("+"))
                    throw new IllegalArgumentException("syntax error");
            }

            final String start = startStr.replaceAll("^[(\\[]", "");
            final String end = endStr.replaceAll("^[(\\[]", "");
            ValueWithTTL.ZSet.Item[] r = handleSortSet(ctx, key, x -> {
                String[] keys = x.asSet().stream().map(ValueWithTTL.ZSet.Item::getKey).toArray(String[]::new);
                int a = start.equals("-") ? 0 : searchValueOrBoundary(keys, start, BoundaryType.UPPER_BOUNDARY, startInclusive);
                int b = end.equals("+") ? keys.length - 1 : searchValueOrBoundary(keys, end, BoundaryType.LOWER_BOUNDARY, endInclusive);
                return Arrays.stream(Arrays.copyOfRange(keys, a, b + 1))
                        .map(i -> ValueWithTTL.ofItem(i, 0.0))
                        .toArray(ValueWithTTL.ZSet.Item[]::new);
            });

            if (offset != null) {
                int o = Integer.parseInt(offset);
                if (o < 0) {
                    return new ValueWithTTL.ZSet().asSet();
                }
                int c = Integer.parseInt(count);
                if (c < 0) {
                    return Arrays.stream(r).skip(o).collect(Collectors.toSet());
                } else {
                    return Arrays.stream(r).skip(o).limit(c).collect(Collectors.toSet());
                }
            } else {
                return Arrays.stream(r).collect(Collectors.toSet());
            }
        }
    }

    enum BoundaryType {
        LOWER_BOUNDARY,
        UPPER_BOUNDARY
    }

    private static int searchValueOrBoundary(String[] array, String key, BoundaryType type, boolean inclusive) {
        int low = 0;
        int high = array.length - 1;
        int resultIndex = -1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            if (array[mid].compareTo(key) == 0) {
                return inclusive? mid :
                        type == BoundaryType.LOWER_BOUNDARY? mid - 1 : mid + 1; // Value found
            } else if (array[mid].compareTo(key) < 0) {
                resultIndex = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (type == BoundaryType.LOWER_BOUNDARY) {
            return resultIndex;
        } else {
            return high;
        }
    }

    Long zsetRank(Context ctx, String key, String member, boolean isRev) {
        return handleSortSet(ctx, key, x -> {
            if (x.size() == 0 || !x.containsKey(member))
                return null;

            ValueWithTTL.ZSet.Item i = x.getItem(member);
            int n = x.asSet().headSet(i, true).size();
            return (long) (isRev? x.size() - n : n - 1);
        });
    }

    long zsetRemByKey(Context ctx, String key, String[] members) {
        return handleSortSet(ctx, key, x -> {
            if (x.size() == 0)
                return 0L;

            long deleted = 0;
            for (String m: members) {
                deleted += x.removeKey(m)? 1 : 0;
            }
            return deleted;
        });
    }

    long zsetRemByRange(Context ctx, String key, String min, String max, boolean byScoreOrLex) {
        Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, byScoreOrLex, false, null, null);
        return handleSortSet(ctx, key, x -> {
            zset.forEach(x::remove);
            return (long) zset.size();
        });
    }

    long zsetRemByRank(Context ctx, String key, String min, String max) {
        long a = Integer.parseInt(min);
        if (a < 0) {
            a += ctx.getStore().size();
            if (a < 0)
                a = 0;
        }
        long b = Integer.parseInt(max);
        if (b < 0) {
            b += ctx.getStore().size();
            if (b < 0)
                b = 0;
        }

        if (a > b)
            return 0;

        Long[] in = new Long[2];
        in[0] = a;
        in[1] = b;
        return handleSortSet(ctx, key, x -> {
            if (in[0] >= x.size())
                return 0L;
            if (in[1] >= x.size())
                in[1] = x.size() - 1L;

            AtomicInteger c = new AtomicInteger();
            x.asSet().stream().skip(in[1]).limit(in[1] - in[0] + 1).forEach(e -> { x.asSet().remove(e); c.getAndIncrement(); });
            return c.longValue();
        });
    }

    String[] zsetPop(String count, ValueWithTTL.ZSet zSet, Function<ValueWithTTL.ZSet, ValueWithTTL.ZSet.Item> func) {
        int c = 1;
        if (count != null) {
            c = Integer.parseInt(count);
            if (c <= 0)
                throw new IllegalArgumentException("count must be positive");
        }

        List<String> r = new ArrayList<>();
        while (c-- > 0 && zSet.size() > 0) {
            ValueWithTTL.ZSet.Item item = func.apply(zSet);
            r.add(item.getKey());
            r.add(item.getScore().toString());
        }

        return r.toArray(new String[0]);
    }

    private RespType toRespType(Set<ValueWithTTL.ZSet.Item> zset, boolean withScores) {
        return RespType.ofArray(zset.stream()
                .map(x -> {
                    if (withScores) {
                        return new String[] { x.getKey(), x.getScore().toString() };
                    } else {
                        return new String[] { x.getKey() };
                    }
                })
                .flatMap(Arrays::stream)
                .toArray(String[]::new)
        );
    }

    SortedSetHandler(InMemorySharedStore sharedStore) {
        super("SortedSet", sharedStore);

        commands.put("BZMPOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BZMPOP timeout numkeys key [key ...] [COUNT count]",
                        "Removes and returns a member by score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped."
                        , Command.Part.ofValue("timeout")
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("BZPOPMAX",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BZPOPMAX key [key ...] timeout",
                        "Removes and returns the member with the highest score from one or more sorted sets. Blocks until a member available otherwise.  Deletes the sorted set if the last element was popped."
                        , Command.Part.ofListValue("key") // FIXME: value after var-length list
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("BZPOPMIN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BZPOPMIN key [key ...] timeout",
                        "Removes and returns the member with the lowest score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped."
                        , Command.Part.ofListValue("key") // FIXME: value after var-length list
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("ZADD",
                new Command(
                        (ctx, args) -> {
                            int ret = handleSortSet(ctx, args.valueWithName("key"), x -> {
                                String[] sm = args.valueListDefault();
                                int added = 0;
                                int changed = 0;
                                for (int i = 0; i < sm.length / 2; i++) {
                                    String s = sm[i * 2];
                                    String m = sm[i * 2 + 1];
                                    ValueWithTTL.ZSet.Item item = ValueWithTTL.ofItem(m, Double.parseDouble(s));
                                    if (args.hasOption("NX") && x.contains(item))
                                        continue;

                                    if (args.hasOption("XX") && !x.contains(item))
                                        continue;

                                    if (x.contains(item)) {
                                        changed++;
                                    } else {
                                        added++;
                                    }
                                    x.add(item);
                                }
                                return args.hasOption("CH")? changed : added;
                            });
                            return RespType.ofLong(ret);
                        },
                        "ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member  ...]",
                        "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("NX"),
                                Command.Part.ofOptionNamedSimple("XX"))
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("GT"),
                                Command.Part.ofOptionNamedSimple("LT"))
                        , Command.Part.ofOptionNamedSimple("CH")
                        , Command.Part.ofOptionNamedSimple("INCR")
                        , Command.Part.ofListValue("score", "member")
                )
        );
        commands.put("ZCARD",
                new Command(
                        (ctx, args) -> {
                            int ret = handleSortSet(ctx, args.valueWithName("key"), ValueWithTTL.ZSet::size);
                            return RespType.ofLong(ret);
                        },
                        "ZCARD key",
                        "Returns the number of members in a sorted set."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("ZCOUNT",
                new Command(
                        (ctx, args) -> {
                            int ret = handleSortSet(ctx, args.valueWithName("key"),
                                    x -> rankByScore(x, args.valueWithName("min"), args.valueWithName("max")).size());
                            return RespType.ofLong(ret);
                        },
                        "ZCOUNT key min max",
                        "Returns the count of members in a sorted set that have scores within a range."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                )
        );
        commands.put("ZDIFF",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet diff = zsetDiff(ctx, args.valueListDefault());
                            return toRespType(diff.asSet(), args.hasOption("WITHSCORES"));
                        },
                        "ZDIFF numkeys key [key ...] [WITHSCORES]",
                        "Returns the difference between multiple sorted sets."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                )
        );
        commands.put("ZDIFFSTORE",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet diff = zsetDiff(ctx, args.valueListDefault());
                            ctx.getStore().compute(args.valueWithName("destination"), (k, v) -> ValueWithTTL.ofSortedSetValue(diff, v));
                            return RespType.ofLong(diff.size());
                        },
                        "ZDIFFSTORE destination numkeys key [key ...]",
                        "Stores the difference of multiple sorted sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                )
        );
        commands.put("ZINCRBY",
                new Command(
                        (ctx, args) -> {
                            Double ret = handleSortSet(ctx, args.valueWithName("key"), x -> {
                                String k = args.valueWithName("member");
                                double incr = Double.parseDouble(args.valueWithName("increment"));
                                ValueWithTTL.ZSet.Item item = ValueWithTTL.ofItem(k, incr);
                                if (x.contains(item)) {
                                    x.add(ValueWithTTL.ofItem(k, incr + x.getScore(k)));
                                } else {
                                    x.add(item);
                                }
                                return x.getScore(k);
                            });
                            return RespType.ofBulkString(ret.toString());
                        },
                        "ZINCRBY key increment member",
                        "Increments the score of a member in a sorted set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("increment")
                        , Command.Part.ofValue("member")
                )
        );
        commands.put("ZINTER",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet zset = zsetInter(ctx, args.valueListDefault(), args.optionWithName("WEIGHTS"),
                                    args.optionWithName("AGGREGATE")[0]);
                            return toRespType(zset.asSet(), args.hasOption("WITHSCORES"));
                        },
                        "ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]",
                        "Returns the intersect of multiple sorted sets."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedVarList("WEIGHTS", "weight")
                        , Command.Part.ofOptionNamedSimple("AGGREGATE")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                )
        );
        commands.put("ZINTERCARD",
                new Command(
                        (ctx, args) -> {
                            long ret = zsetInterCard(ctx, args.valueListDefault(), args.valueWithName("LIMIT"));
                            return RespType.ofLong(ret);
                        },
                        "ZINTERCARD numkeys key [key ...] [LIMIT limit]",
                        "Returns the number of members of the intersect of multiple sorted sets."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "limit")
                )
        );
        commands.put("ZINTERSTORE",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet zset = zsetInter(ctx, args.valueListDefault(), args.optionWithName("WEIGHTS"),
                                    args.optionWithName("AGGREGATE")[0]);
                            ctx.getStore().compute(args.valueWithName("destination"), (k, v) -> ValueWithTTL.ofSortedSetValue(zset, v));
                            return RespType.ofLong(zset.size());
                        },
                        "ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]",
                        "Stores the intersect of multiple sorted sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedVarList("WEIGHTS", "weight")
                        , Command.Part.ofOptionNamedTerms("AGGREGATE", "SUM", "MIN", "MAX")
                )
        );
        commands.put("ZLEXCOUNT",
                new Command(
                        (ctx, args) -> {
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, args.valueWithName("key"), min, max, true, false, null, null);
                            return RespType.ofLong(zset.size());
                        },
                        "ZLEXCOUNT key min max",
                        "Returns the number of members in a sorted set within a lexicographical range."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                )
        );
        commands.put("ZMPOP",
                new Command(
                        (ctx, args) -> {
                            boolean byMinOrMax = args.termAtPos(1).equals("MIN");
                            Map<String, List<ValueWithTTL.ZSet.Item>> ret = new HashMap<>();
                            String[] keys = args.valueListDefault();
                            Long[] count = new Long[1];
                            count[0] = Long.parseLong(args.optionWithName("COUNT")[0]);
                            for (String key: keys) {
                                List<ValueWithTTL.ZSet.Item> r = handleSortSet(ctx, key, x ->  {
                                    if (x.size() == 0)
                                        return null;

                                    List<ValueWithTTL.ZSet.Item> t = new ArrayList<>();
                                    while (x.size() > 0 && count[0] > 0) {
                                        ValueWithTTL.ZSet.Item i = byMinOrMax? x.first() : x.last();
                                        t.add(i);
                                        x.remove(i);
                                        count[0]--;
                                    }
                                    return t;
                                });

                                if (r != null) {
                                    ret.put(key, r);
                                    break;
                                }
                            }

                            RespType[] r = new RespType[ret.size() * 2];
                            int n = 0;
                            for (String k: ret.keySet()) {
                                r[n*2] = RespType.ofBulkString(k);
                                r[n*2+1] = RespType.ofArray(ret.get(k)
                                            .stream()
                                        .map(x -> RespType.ofArray(RespType.ofBulkString(x.getKey()),
                                                RespType.ofBulkString(x.getScore().toString())))
                                        .toArray(RespType[]::new))
                                ;
                                n++;
                            }
                            return RespType.ofArray(r);
                        },
                        "ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]",
                        "Returns the highest- or lowest-scoring members from one or more sorted sets after removing them. Deletes the sorted set if the last member was popped."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofTermValue(1, "MIN", "MAX")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("ZMSCORE",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleSortSet(ctx, args.valueWithName("key"), x -> {
                                String[] ms = args.valueListDefault();
                                String[] r = new String[ms.length];
                                for (int i = 0; i < ms.length; i++)
                                    r[i] = x.getScore(ms[i]) != null? x.getScore(ms[i]).toString() : null;
                                return r;
                            });
                            return RespType.ofArray(ret);
                        },
                        "ZMSCORE key member [member ...]",
                        "Returns the score of one or more members in a sorted set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("member")
                )
        );
        commands.put("ZPOPMAX",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleSortSet(ctx, args.valueWithName("key"),
                                    x -> zsetPop(args.optionValueAnonymous(), x, ValueWithTTL.ZSet::last));
                            return RespType.ofArray(ret);
                        },
                        "ZPOPMAX key [count]",
                        "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("ZPOPMIN",
                new Command(
                        (ctx, args) -> {
                            String[] ret = handleSortSet(ctx, args.valueWithName("key"),
                                    x -> zsetPop(args.optionValueAnonymous(), x, ValueWithTTL.ZSet::first));
                            return RespType.ofArray(ret);
                        },
                        "ZPOPMIN key [count]",
                        "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("ZRANDMEMBER",
                new Command(
                        (ctx, args) -> handleSortSet(ctx, args.valueWithName("key"),
                                x -> handleRandomSelect(x.getKeySet(), args.optionValueAnonymous(),
                                        args.optionValueTerms().contains("WITHSCORES"), k -> x.getScore(k).toString())),
                        "ZRANDMEMBER key [count [WITHSCORES]]",
                        "Returns one or more random members from a sorted set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymousWithTerm("count", "WITHSCORES")
                )
        );
        commands.put("ZRANGE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("start");
                            String max = args.valueWithName("stop");
                            boolean byScoreOrLex = !args.hasOption("BYLEX");
                            boolean withScores = args.hasOption("WITHSCORES");
                            if (!byScoreOrLex && withScores)
                                throw new IllegalArgumentException("incompatible option: BYLEX && WITHSCORES");
                            boolean isRev = args.hasOption("REV");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, byScoreOrLex, isRev, offset, count);
                            return toRespType(zset, byScoreOrLex && withScores);
                        },
                        "ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]",
                        "Returns members in a sorted set within a range of indexes."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("stop")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("BYSCORE"),
                                Command.Part.ofOptionNamedSimple("BYLEX"))
                        , Command.Part.ofOptionNamedSimple("REV")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                )
        );
        commands.put("ZRANGEBYLEX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, true, false, offset, count);
                            return toRespType(zset, false);
                        },
                        "ZRANGEBYLEX key min max [LIMIT offset count]",
                        "Returns members in a sorted set within a lexicographical range."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                )
        );
        commands.put("ZRANGEBYSCORE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, true, false, offset, count);
                            return toRespType(zset, args.hasOption("WITHSCORES"));
                        },
                        "ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]",
                        "Returns members in a sorted set within a range of scores."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                )
        );
        commands.put("ZRANGESTORE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("src");
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            boolean byScoreOrLex = !args.hasOption("BYLEX");
                            boolean isRev = args.hasOption("REV");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, byScoreOrLex, isRev, offset, count);
                            ctx.getStore().compute(args.valueWithName("dst"), (k, v) -> ValueWithTTL.ofSortedSetValue(zset, v));
                            return RespType.ofLong(zset.size());
                        },
                        "ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset  count]",
                        "Stores a range of members from sorted set in a key."
                        , Command.Part.ofValue("dst")
                        , Command.Part.ofValue("src")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                        , Command.Part.ofOptionChoice(
                                Command.Part.ofOptionNamedSimple("BYSCORE"),
                                Command.Part.ofOptionNamedSimple("BYLEX"))
                        , Command.Part.ofOptionNamedSimple("REV")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                )
        );
        commands.put("ZRANK",
                new Command(
                        (ctx, args) -> {
                            Long rank = zsetRank(ctx, args.valueWithName("key"), args.valueWithName("member"), false);
                            if (rank == null)
                                return RespType.NullBulkString();

                            if (args.hasOption("WITHSCORE")) {
                                Double s = handleSortSet(ctx, args.valueWithName("key"),
                                        x -> x.getScore(args.valueWithName("member")));
                                return RespType.ofArray(RespType.ofLong(rank), RespType.ofBulkString(s.toString()));
                            } else {
                                return RespType.ofLong(rank);
                            }
                        },
                        "ZRANK key member [WITHSCORE]",
                        "Returns the index of a member in a sorted set ordered by ascending scores."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("member")
                        , Command.Part.ofOptionNamedSimple("WITHSCORE")
                )
        );
        commands.put("ZREM",
                new Command(
                        (ctx, args) -> {
                            long n = zsetRemByKey(ctx, args.valueWithName("key"), args.valueListDefault());
                            return RespType.ofLong(n);
                        },
                        "ZREM key member [member ...]",
                        "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("member")
                )
        );
        commands.put("ZREMRANGEBYLEX",
                new Command(
                        (ctx, args) -> {
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            long n = zsetRemByRange(ctx, args.valueWithName("key"), min, max, false);
                            return RespType.ofLong(n);
                        },
                        "ZREMRANGEBYLEX key min max",
                        "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                )
        );
        commands.put("ZREMRANGEBYRANK",
                new Command(
                        (ctx, args) -> {
                            String min = args.valueWithName("start");
                            String max = args.valueWithName("stop");
                            long n = zsetRemByRank(ctx, args.valueWithName("key"), min, max);
                            return RespType.ofLong(n);
                        },
                        "ZREMRANGEBYRANK key start stop",
                        "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("stop")
                )
        );
        commands.put("ZREMRANGEBYSCORE",
                new Command(
                        (ctx, args) -> {
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            long n = zsetRemByRange(ctx, args.valueWithName("key"), min, max, true);
                            return RespType.ofLong(n);
                        },
                        "ZREMRANGEBYSCORE key min max",
                        "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofValue("max")
                )
        );
        commands.put("ZREVRANGE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("start");
                            String max = args.valueWithName("stop");

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, true, true, null, null);
                            return toRespType(zset, args.hasOption("WITHSCORES"));
                        },
                        "ZREVRANGE key start stop [WITHSCORES]",
                        "Returns members in a sorted set within a range of indexes in reverse order."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("stop")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                )
        );
        commands.put("ZREVRANGEBYLEX",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, false, true, offset, count);
                            return toRespType(zset, false);
                        },
                        "ZREVRANGEBYLEX key max min [LIMIT offset count]",
                        "Returns members in a sorted set within a lexicographical range in reverse order."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("max")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                )
        );
        commands.put("ZREVRANGEBYSCORE",
                new Command(
                        (ctx, args) -> {
                            String key = args.valueWithName("key");
                            String min = args.valueWithName("min");
                            String max = args.valueWithName("max");
                            String offset = null, count = null;
                            if (args.hasOption("LIMIT")) {
                                offset = args.optionWithName("LIMIT")[0];
                                count = args.optionWithName("LIMIT")[1];
                            }

                            Set<ValueWithTTL.ZSet.Item> zset = zsetRange(ctx, key, min, max, false, true, offset, count);
                            return toRespType(zset, args.hasOption("WITHSCORES"));
                        },
                        "ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]",
                        "Returns members in a sorted set within a range of scores in reverse order."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("max")
                        , Command.Part.ofValue("min")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                        , Command.Part.ofOptionNamedSimple("LIMIT", "offset", "count")
                )
        );
        commands.put("ZREVRANK",
                new Command(
                        (ctx, args) -> {
                            Long rank = zsetRank(ctx, args.valueWithName("key"), args.valueWithName("member"), true);
                            if (rank == null)
                                return RespType.NullBulkString();

                            if (args.hasOption("WITHSCORE")) {
                                Double s = handleSortSet(ctx, args.valueWithName("key"),
                                        x -> x.getScore(args.valueWithName("member")));
                                return RespType.ofArray(RespType.ofLong(rank), RespType.ofBulkString(s.toString()));
                            } else {
                                return RespType.ofLong(rank);
                            }
                        },
                        "ZREVRANK key member [WITHSCORE]",
                        "Returns the index of a member in a sorted set ordered by descending scores."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("member")
                        , Command.Part.ofOptionNamedSimple("WITHSCORE")
                )
        );
        commands.put("ZSCAN",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "ZSCAN key cursor [MATCH pattern] [COUNT count]",
                        "Iterates over members and scores of a sorted set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("cursor")
                        , Command.Part.ofOptionNamedSimple("MATCH", "pattern")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("ZSCORE",
                new Command(
                        (ctx, args) -> {
                            Double s = handleSortSet(ctx, args.valueWithName("key"), x -> x.getScore(args.valueWithName("member")));
                            return RespType.ofBulkString(s.toString());
                        },
                        "ZSCORE key member",
                        "Returns the score of a member in a sorted set."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("member")
                )
        );
        commands.put("ZUNION",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet zset = zsetUnion(ctx, args.valueListDefault(),
                                    args.optionWithName("WEIGHTS"), args.optionWithName("AGGREGATE")[0]);
                            return toRespType(zset.asSet(), args.hasOption("WITHSCORES"));
                        },
                        "ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]",
                        "Returns the union of multiple sorted sets."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedVarList("WEIGHTS", "weight")
                        , Command.Part.ofOptionNamedTerms("AGGREGATE", "SUM", "MIN", "MAX")
                        , Command.Part.ofOptionNamedSimple("WITHSCORES")
                )
        );
        commands.put("ZUNIONSTORE",
                new Command(
                        (ctx, args) -> {
                            ValueWithTTL.ZSet zset = zsetUnion(ctx, args.valueListDefault(),
                                    args.optionWithName("WEIGHTS"), args.optionWithName("AGGREGATE")[0]);
                            ctx.getStore().compute(args.valueWithName("dst"), (k, v) -> ValueWithTTL.ofSortedSetValue(zset, v));
                            return RespType.ofLong(zset.size());
                        },
                        "ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]",
                        "Stores the union of multiple sorted sets in a key."
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedVarList("WEIGHTS", "weight")
                        , Command.Part.ofOptionNamedTerms("AGGREGATE", "SUM", "MIN", "MAX")
                )
        );
    }
}
