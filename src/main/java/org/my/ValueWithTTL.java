package org.my;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

@lombok.Getter
@lombok.Setter
public class ValueWithTTL {
    private final ValueType type;
    private Object value;
    private Long expiredAt; // unix-time point in milliseconds, local time

    public ValueWithTTL(ValueType type, Object value) {
        this(type, value, null);
    }

    public ValueWithTTL(Object value, ValueWithTTL copyFrom) {
        this(copyFrom.getType(), value, copyFrom.getExpiredAt());
    }

    public ValueWithTTL(ValueType type, Object value, Long expiredAt) {
        this.type = type;
        this.value = value;
        this.expiredAt = expiredAt;
    }

    public static ValueWithTTL ofString(String value) {
        return new ValueWithTTL(ValueType.String, value);
    }

    public static ValueWithTTL ofString(String value, ValueWithTTL copyFrom) {
        return new ValueWithTTL(ValueType.String, value, copyFrom.getExpiredAt());
    }

    public static ValueWithTTL ofSetValue() {
        return ofSetValue(getSet());
    }

    public static ValueWithTTL ofSetValue(Set<String> value) {
        return new ValueWithTTL(ValueType.Set, value);
    }

    public static ValueWithTTL ofHashValue() {
        return ofHashValue(getHash());
    }

    public static ValueWithTTL ofHashValue(Map<String, Object> value) {
        return new ValueWithTTL(ValueType.Hash, value);
    }

    public static ValueWithTTL ofListValue() {
        return ofListValue(getList());
    }

    public static ValueWithTTL ofListValue(LinkedList<String> value) {
        return new ValueWithTTL(ValueType.List, value);
    }

    public static ValueWithTTL ofSortedSetValue() {
        return ofSortedSetValue(getSortedSet());
    }

    public static ValueWithTTL ofSortedSetValue(ZSet value) {
        return new ValueWithTTL(ValueType.SortedSet, value);
    }

    public static ValueWithTTL ofSortedSetValue(Set<ZSet.Item> value, ValueWithTTL copyFrom) {
        return ofSortedSetValue(new ZSet(value), copyFrom);
    }

    public static ValueWithTTL ofSortedSetValue(ZSet value, ValueWithTTL copyFrom) {
        return new ValueWithTTL(ValueType.SortedSet, value, copyFrom == null? null : copyFrom.getExpiredAt());
    }

    // the only way to get internal structure, so that we can update this without affect all other codes
    public static Set<String> getSet() {
        return new HashSet<>();
    }

    public static Map<String, Object> getHash() {
        return new HashMap<>();
    }

    public static LinkedList<String> getList() {
        return new LinkedList<>();
    }

    public static ZSet getSortedSet() {
        return new ZSet();
    }

    @SuppressWarnings("unchecked")
    public Set<String> getValueAsSet() {
        if (type != ValueType.Set) {
            throw new IllegalArgumentException("expect SET type");
        }

        return (Set<String>) value;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getValueAsHash() {
        if (type != ValueType.Hash) {
            throw new IllegalArgumentException("expect Hash type");
        }

        return (Map<String, String>) value;
    }

    @SuppressWarnings("unchecked")
    public LinkedList<String> getValueAsList() {
        if (type != ValueType.List) {
            throw new IllegalArgumentException("expect List type");
        }

        return (LinkedList<String>) value;
    }

    public ZSet getValueAsSortedSet() {
        if (type != ValueType.SortedSet) {
            throw new IllegalArgumentException("expect List type");
        }

        return (ZSet) value;
    }

    public enum ValueType { String, List, Hash, Set, SortedSet, Stream }

    @lombok.NoArgsConstructor
    public static class ZSet {
        private final ConcurrentSkipListSet<ZSet.Item> impl = new ConcurrentSkipListSet<>();
        private final HashMap<String, Double> scores = new HashMap<>();

        public ZSet(Set<ZSet.Item> set) {
            for (ZSet.Item s: set) {
                impl.add(s);
                scores.put(s.getKey(), s.getScore());
            }
        }

        public boolean add(ZSet.Item e) {
            scores.put(e.getKey(), e.getScore());
            return impl.add(e);
        }

        public boolean containsKey(String k) {
            return scores.containsKey(k);
        }

        public boolean removeKey(String key) {
            scores.remove(key);
            return impl.remove(new Item(key, 0d));
        }

        public Double getScore(String key) {
            return scores.get(key);
        }

        public Item getItem(String key) {
            return new Item(key, getScore(key));
        }

        public Set<String> getKeySet() {
            return impl.stream().map(Item::getKey).collect(Collectors.toSet());
        }

        public int size() { return impl.size(); }
        public ConcurrentSkipListSet<ZSet.Item> asSet() { return impl; }
        public ZSet.Item first() { return impl.first(); }
        public ZSet.Item last() { return impl.last(); }
        public boolean remove(ZSet.Item e) { return impl.remove(e) && scores.remove(e.getKey()) != null; }
        public boolean contains(ZSet.Item e) { return impl.contains(e); }

        @lombok.AllArgsConstructor
        @lombok.Getter
        public static class Item implements Comparable<Item> {
            String key;
            Double score;

            @Override
            public int compareTo(Item o) {
                int keyComparison = this.key.compareTo(o.key);
                if (keyComparison == 0)
                    return 0;

                int scoreComparison = Double.compare(this.score, o.score);
                return scoreComparison != 0? scoreComparison : keyComparison;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                Item item = (Item) obj;
                return key.equals(item.key);
            }

            @Override
            public int hashCode() {
                return key.hashCode();
            }
        }
    }

    public static ZSet.Item ofItem(double score) {
        return ofItem("", score);
    }

    public static ZSet.Item ofItem(String key, double score) {
        return new ValueWithTTL.ZSet.Item(key, score);
    }
}
