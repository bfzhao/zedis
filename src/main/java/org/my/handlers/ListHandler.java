package org.my.handlers;

import org.my.*;
import org.my.zedis.InMemorySharedStore;
import org.my.zedis.RedisCommandHandler;
import org.my.zedis.RespType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Component
public class ListHandler extends RedisCommandHandler {
    private final ValueWithTTL.ValueType valueType = ValueWithTTL.ValueType.List;

    @SuppressWarnings("unchecked")
    private <T> T handleList(Context ctx, String key, Function<LinkedList<String>, T> func) {
        T[] ret = (T[]) new Object[1];
        ret[0] = null;

        ctx.getStore().compute(key, (k, v) -> {
            if (v == null) {
                v = ValueWithTTL.ofListValue();
            }

            assertValueType(valueType, v);
            LinkedList<String> list = v.getValueAsList();
            T t = func.apply(list);
            ret[0] = t;
            return list.size() == 0? null : v;
        });
        return ret[0];
    }

    private String getNthElement(Deque<String> deque, int n) {
        assert(n < deque.size());
        Iterator<String> iterator = deque.iterator();
        int count = 0;
        String e = null;
        while (iterator.hasNext()) {
            e = iterator.next();
            if (count == n) {
                break;
            }
            count++;
        }

        return e;
    }

    private int normalizeIndex(int idx, int listSize) {
        if (idx < 0) {
            idx += listSize;
            if (idx < 0)
                idx = 0;
        }
        return idx;
    }

    private RespType listPop(Context ctx, String key, String count, Function<LinkedList<String>, String> func) {
        String[] ret = handleList(ctx, key, x -> {
            int n = 1;
            if (count != null) {
                n = Integer.parseInt(count);
                if (n < 0)
                    n = x.size();
            }

            List<String> r = new ArrayList<>(n);
            while (x.size() > 0 && n-- > 0) {
                r.add(func.apply(x));
            }
            return r.toArray(new String[0]);
        });

        if (count == null)
            return RespType.ofBulkString(ret[0]);
        else
            return RespType.ofArray(ret);
    }

    private RespType listPush(Context ctx, String key, String[] es, BiConsumer<LinkedList<String>, String> func) {
        int n = handleList(ctx, key, x -> {
            for (String e: es)
                func.accept(x ,e);
            return x.size();
        });
        return RespType.ofLong(n);
    }

    private RespType listPushX(Context ctx, String key, String[] es, BiConsumer<LinkedList<String>, String> func) {
        int n = handleList(ctx, key, x -> {
            if (x.size() == 0)
                return 0;

            for (String e: es)
                func.accept(x, e);
            return x.size();
        });
        return RespType.ofLong(n);
    }

    private RespType listElementMove(Context ctx, String src, String dst, boolean srcLeft, boolean dstLeft) {
        String ret = handleList(ctx, src, x -> {
            if (x.size() == 0)
                return null;

            String e = srcLeft? x.removeFirst() : x.removeLast();
            handleList(ctx, dst, y -> {
                if (dstLeft) {
                    y.addFirst(e);
                } else {
                    y.addLast(e);
                }
                return 0;
            });
            return e;
        });
        return RespType.ofBulkString(ret);
    }

    ListHandler(InMemorySharedStore sharedStore) {
        super("List", sharedStore);

        commands.put("BLMOVE",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BLMOVE source destination timeout",
                        "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was moved."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("BLMPOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BLMPOP timeout numkeys key [key ...] [COUNT count]",
                        "Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes the list if the last element was popped."
                        , Command.Part.ofValue("timeout")
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("BLPOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BLPOP key [key ...] timeout",
                        "Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped."
                        , Command.Part.ofListValue("key")
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("BRPOP",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BRPOP key [key ...] timeout",
                        "Removes and returns the last element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped."
                        , Command.Part.ofListValue("key")
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("BRPOPLPUSH",
                new Command(
                        (ctx, args) -> {
                            return RespType.ofError("not implemented");
                        },
                        "BRPOPLPUSH source destination timeout",
                        "Pops an element from a list, pushes it to another list and returns it. Block until an element is available otherwise. Deletes the list if the last element was popped."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofValue("timeout")
                )
        );
        commands.put("LINDEX",
                new Command(
                        (ctx, args) -> {
                            String ret = handleList(ctx, args.valueWithName("key"), x -> {
                                int idx = Integer.parseInt(args.valueWithName("index"));
                                return idx >= x.size()? null : getNthElement(x, idx);
                            });
                            return RespType.ofBulkString(ret);
                        },
                        "LINDEX key index",
                        "Returns an element from a list by its index."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("index")
                )
        );
        commands.put("LINSERT",
                new Command(
                        (ctx, args) -> {
                            int ret = handleList(ctx, args.valueWithName("key"), x -> {
                                if (x.size() == 0)
                                    return 0;
                                int idx = x.indexOf(args.valueWithName("pivot"));
                                if (idx == -1)
                                    return -1;

                                String e = args.valueWithName("element");
                                if (args.termAtPos(0).equals("BEFORE")) {
                                    x.add(idx, e);
                                } else {
                                    // AFTER case
                                    if (idx == x.size())
                                        x.addLast(e);
                                    else
                                        x.add(++idx, e);
                                }
                                return x.size();
                            });
                            return RespType.ofLong(ret);
                        },
                        "LINSERT key <BEFORE | AFTER> pivot element",
                        "Inserts an element before or after another element in a list."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofTermValue(1, "BEFORE", "AFTER")
                        , Command.Part.ofValue("pivot")
                        , Command.Part.ofValue("element")
                )
        );
        commands.put("LLEN",
                new Command(
                        (ctx, args) -> {
                            int n = handleList(ctx, args.valueWithName("key"), LinkedList::size);
                            return RespType.ofLong(n);
                        },
                        "LLEN key",
                        "Returns the length of a list."
                        , Command.Part.ofValue("key")
                )
        );
        commands.put("LMOVE",
                new Command(
                        (ctx, args) -> listElementMove(ctx, args.valueWithName("source"), args.valueWithName("destination"),
                                args.termAtPos(0).equals("LEFT"), args.termAtPos(1).equals("LEFT")),
                        "LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>",
                        "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                        , Command.Part.ofTermValue(2, "LEFT", "RIGHT")
                        , Command.Part.ofTermValue(3, "LEFT", "RIGHT")
                )
        );
        commands.put("LMPOP",
                new Command(
                        (ctx, args) -> {
                            boolean leftOrRight = args.termAtPos(1).equals("LEFT");
                            Map<String, List<String>> ret = new HashMap<>();
                            String[] keys = args.valueListDefault();
                            Long[] count = new Long[1];
                            count[0] = Long.parseLong(args.optionWithName("COUNT")[0]);
                            for (String key: keys) {
                                List<String> r = handleList(ctx, key, x ->  {
                                    if (x.size() == 0)
                                        return null;

                                    List<String> t = new ArrayList<>();
                                    while (x.size() > 0 && count[0] > 0) {
                                        String i = leftOrRight? x.removeFirst() : x.removeLast();
                                        t.add(i);
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
                                r[n*2+1] = RespType.ofArray(ret.get(k).toArray(new String[0]));
                                n++;
                            }
                            return RespType.ofArray(r);
                        },
                        "LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]",
                        "Returns multiple elements from a list after removing them. Deletes the list if the last element was popped."
                        , Command.Part.ofFixedLengthListValue("numkeys", "key")
                        , Command.Part.ofTermValue(1, "LEFT", "RIGHT")
                        , Command.Part.ofOptionNamedSimple("COUNT", "count")
                )
        );
        commands.put("LPOP",
                new Command(
                        (ctx, args) -> listPop(ctx, args.valueWithName("key"), args.optionValueAnonymous(), LinkedList::pop),
                        "LPOP key [count]",
                        "Returns the first elements in a list after removing it. Deletes the list if the last element was popped."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("LPOS",
                new Command(
                        (ctx, args) -> {
                            Integer[] pos = handleList(ctx, args.valueWithName("key"), x -> {
                                int count = 0;
                                if (args.hasOption("COUNT"))
                                    count = Integer.parseInt(args.optionWithName("COUNT")[0]);
                                if (count < 0)
                                    throw new IllegalArgumentException("COUNT cannot be negative");

                                int rank = 1;
                                if (args.hasOption("RANK"))
                                    rank = Integer.parseInt(args.optionWithName("RANK")[0]);
                                if (rank == 0)
                                    throw new IllegalArgumentException("RANK cannot be zero");

                                int len = 0;
                                if (args.hasOption("MAXLEN"))
                                    len = Integer.parseInt(args.optionWithName("MAXLEN")[0]);
                                if (len < 0)
                                    throw new IllegalArgumentException("MAXLEN cannot be negative");

                                boolean forward = rank > 0;
                                Iterator<String> iter = forward? x.iterator() : x.descendingIterator();
                                int idx = forward? 0 : x.size() - 1;

                                List<Integer> ret = new ArrayList<>();
                                int skipped = 0;
                                rank = Math.abs(rank);
                                int complared = 0;

                                while (iter.hasNext()) {
                                    String e = iter.next();
                                    if (e.equals(args.valueWithName("element"))) {
                                        if (skipped != rank - 1) {
                                            skipped++;
                                        } else {
                                            ret.add(idx);
                                            if (count != 0 && ret.size() >= count)
                                                break;
                                        }
                                    }
                                    idx += forward? 1 : -1;

                                    if (len != 0 && ++complared == len)
                                        break;
                                }
                                return ret.toArray(new Integer[0]);
                            });
                            if (args.optionWithName("COUNT") == null)
                                return pos.length > 0? RespType.ofLong(pos[0]) : RespType.NullBulkString();
                            else
                                return RespType.ofArray(pos);
                        },
                        "LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]",
                        "Returns the index of matching elements in a list."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("element")
                        , Command.Part.ofOptionNamedSimple("RANK", "rank")
                        , Command.Part.ofOptionNamedSimple("COUNT", "num-matches")
                        , Command.Part.ofOptionNamedSimple("MAXLEN", "len")
                )
        );
        commands.put("LPUSH",
                new Command(
                        (ctx, args) -> listPush(ctx, args.valueWithName("key"), args.valueListDefault(), LinkedList::addFirst),
                        "LPUSH key element [element ...]",
                        "Prepends one or more elements to a list. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("element")
                )
        );
        commands.put("LPUSHX",
                new Command(
                        (ctx, args) -> listPushX(ctx, args.valueWithName("key"), args.valueListDefault(), LinkedList::addFirst),
                        "LPUSHX key element [element ...]",
                        "Prepends one or more elements to a list only when the list exists."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("element")
                )
        );
        commands.put("LRANGE",
                new Command(
                        (ctx, args) -> {
                            String [] ret = handleList(ctx, args.valueWithName("key"), x -> {
                                if (x.size() == 0 )
                                    return null;

                                int start = normalizeIndex(Integer.parseInt(args.valueWithName("start")), x.size());
                                if (start > x.size())
                                    return null;

                                int end = normalizeIndex(Integer.parseInt(args.valueWithName("stop")), x.size());
                                if (end > x.size())
                                    end = x.size() - 1;

                                String[] r = new String[end - start + 1];
                                ListIterator<String> iterator = x.listIterator(start);
                                int n = 0;
                                while (iterator.hasNext() && iterator.nextIndex() <= end) {
                                    r[n++] = iterator.next();
                                }
                                return r;
                            });
                            return RespType.ofArray(ret);
                        },
                        "LRANGE key start stop",
                        "Returns a range of elements from a list."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("stop")
                )
        );
        commands.put("LREM",
                new Command(
                        (ctx, args) -> {
                            int n = handleList(ctx, args.valueWithName("key"), x -> {
                                int count = Integer.parseInt(args.valueWithName("count"));
                                String element = args.valueWithName("element");
                                Iterator<String> iter = count >= 0? x.iterator() : x.descendingIterator();
                                count = Math.abs(count) == 0? x.size() : Math.abs(count);

                                int r = 0;
                                while (count > 0 && iter.hasNext()) {
                                    if (iter.next().equals(element)) {
                                        iter.remove();
                                        count--;
                                        r++;
                                    }
                                }
                                return r;
                            });
                            return RespType.ofLong(n);
                        },
                        "LREM key count element",
                        "Removes elements from a list. Deletes the list if the last element was removed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("count")
                        , Command.Part.ofValue("element")
                )
        );
        commands.put("LSET",
                new Command(
                        (ctx, args) -> {
                            handleList(ctx, args.valueWithName("key"), x -> {
                                int index = Integer.parseInt(args.valueWithName("index"));
                                String element = args.valueWithName("element");
                                if (index < 0)
                                    index += x.size();
                                if (index < 0 || index > x.size())
                                    throw new IllegalArgumentException("index out of range");

                                ListIterator<String> listIterator = x.listIterator(index);
                                listIterator.next();
                                listIterator.set(element);
                                return 0;
                            });
                            return RespType.OK();
                        },
                        "LSET key index element",
                        "Sets the value of an element in a list by its index."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("index")
                        , Command.Part.ofValue("element")
                )
        );
        commands.put("LTRIM",
                new Command(
                        (ctx, args) -> {
                            handleList(ctx, args.valueWithName("key"), x -> {
                                if (x.size() == 0)
                                    return 0;

                                int start = Integer.parseInt(args.valueWithName("start"));
                                if (start < 0)
                                    start += x.size();
                                int end = Integer.parseInt(args.valueWithName("stop"));
                                if (end < 0)
                                    end += x.size();

                                if (start > end || start > x.size() || end < 0) {
                                    x.clear();
                                    return 0;
                                }

                                // Remove elements after the end index
                                if (x.size() > end + 1) {
                                    x.subList(end + 1, x.size()).clear();
                                }

                                // Remove elements before the start index
                                if (start > 0) {
                                    x.subList(0, start).clear();
                                }
                                return 0;
                            });
                            return RespType.OK();
                        },
                        "LTRIM key start stop",
                        "Removes elements from both ends a list. Deletes the list if all elements were trimmed."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofValue("start")
                        , Command.Part.ofValue("stop")
                )
        );
        commands.put("RPOP",
                new Command(
                        (ctx, args) -> listPop(ctx, args.valueWithName("key"), args.optionValueAnonymous(), LinkedList::removeLast),
                        "RPOP key [count]",
                        "Returns and removes the last elements of a list. Deletes the list if the last element was popped."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofOptionAnonymous("count")
                )
        );
        commands.put("RPOPLPUSH",
                new Command(
                        (ctx, args) -> listElementMove(ctx, args.valueWithName("source"), args.valueWithName("destination"),
                                false, true),
                        "RPOPLPUSH source destination",
                        "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped."
                        , Command.Part.ofValue("source")
                        , Command.Part.ofValue("destination")
                )
        );
        commands.put("RPUSH",
                new Command(
                        (ctx, args) -> listPush(ctx, args.valueWithName("key"), args.valueListDefault(), LinkedList::addLast),
                        "RPUSH key element [element ...]",
                        "Appends one or more elements to a list. Creates the key if it doesn't exist."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("element")
                )
        );
        commands.put("RPUSHX",
                new Command(
                        (ctx, args) -> listPushX(ctx, args.valueWithName("key"), args.valueListDefault(), LinkedList::addLast),
                        "RPUSHX key element [element ...]",
                        "Appends an element to a list only when the list exists."
                        , Command.Part.ofValue("key")
                        , Command.Part.ofListValue("element")
                )
        );
    }
}
