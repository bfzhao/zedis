package org.my.zedis;

import org.my.Argument;
import org.my.Command;
import org.my.Context;
import org.my.ValueWithTTL;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

@lombok.Getter
@lombok.extern.slf4j.Slf4j
public abstract class RedisCommandHandler {
    private final String name;
    private final InMemorySharedStore store;

    protected final Map<String, Command> commands = new HashMap<>();

    protected RedisCommandHandler(String name, InMemorySharedStore store) {
        this.name = name;
        this.store = store;
    }

    protected static void assertValueType(ValueWithTTL.ValueType type, ValueWithTTL value) {
        if (value != null && value.getType() != type) {
            String err = String.format("Value type is %s but expect %s", value.getType().toString(), type.toString());
            throw new IllegalArgumentException(err);
        }
    }

    public Set<String> getCommands() {
        return commands.keySet();
    }

    public RespType handle(String name, RespType[] args, String clientKey, int dbIdx) {
        try {
            Command command = commands.get(name);
            Argument argument = command.parseArguments(args);
            return command.getFunc().apply(new Context(clientKey, store.getDB(dbIdx)), argument);
        } catch (Exception e) {
            log.error("exception: ", e);
            return RespType.ofError(e.getMessage());
        }
    }

    protected RespType handleRandomSelect(Set<String> keySet, String countStr, boolean withValue, Function<String, String> getValueFunc) {
        if (keySet.size() == 0)
            return RespType.NullBulkString();

        Random rand = new Random();
        if (countStr != null) {
            int count = Integer.parseInt(countStr);
            if (keySet.size() == 0 || count == 0)
                return RespType.ofArray(new String[0]);

            if (count > 0) {
                Set<String> copy = new HashSet<>(keySet); // we need a copy as we will do DELETE on this
                List<String> selected = new ArrayList<>(withValue? count * 2: count);
                for (int i = 0; i < count; i++) {
                    if (copy.size() == 0)
                        break;

                    String[] keys = copy.toArray(new String[0]);
                    int idx = rand.nextInt(keys.length);
                    String k = keys[idx];
                    selected.add(k);
                    if (withValue)
                        selected.add(getValueFunc.apply(k));
                    copy.remove(k);
                }
                return RespType.ofArray(selected.toArray(new String[0]));
            } else {
                int c = Math.abs(count);
                String[] keys = keySet.toArray(new String[0]);
                return RespType.ofArray(IntStream.range(0, c)
                        .mapToLong(i -> rand.nextInt(keys.length))
                        .mapToObj(i -> keys[(int) i])
                        .map(k -> withValue? new String[]{k, getValueFunc.apply(k)} : new String[]{k})
                        .flatMap(Arrays::stream)
                        .map(RespType::ofBulkString)
                        .toArray(RespType[]::new)
                );
            }
        } else {
            if (keySet.size() == 0) {
                return RespType.NullBulkString();
            } else {
                String[] keys = keySet.toArray(new String[0]);
                int randomIndex = rand.nextInt(keys.length);
                return RespType.ofString(keys[randomIndex]);
            }
        }
    }
}
