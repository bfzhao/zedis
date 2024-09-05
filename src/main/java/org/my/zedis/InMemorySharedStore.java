package org.my.zedis;

import org.my.ValueWithTTL;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class InMemorySharedStore {
    public static final int MAX_DB_SIZE = 16;

    private final Map<String, ValueWithTTL>[] store;

    @SuppressWarnings("unchecked")
    public InMemorySharedStore() {
        int initiateMapSize = 20;
        this.store= (HashMap<String, ValueWithTTL>[]) new HashMap[MAX_DB_SIZE];
        for (int i = 0; i < store.length; i++) {
            store[i] = new HashMap<>(initiateMapSize);
        }
    }

    public Map<String, ValueWithTTL> getDB(int dbIdx) {
        return store[dbIdx];
    }

    public void flushAll() {
        Arrays.stream(store).forEach(Map::clear);
    }
}
