package pl.symentis.mapreduce.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class HashMapOutput<K, V> implements MapperOutput<K, V> {

    private final HashMap<K, List<V>> map = new HashMap<>();

    @Override
    public void emit(K k, V v) {
        map.compute(k, (key, oldValue) -> {
            List<V> newValue = oldValue;
            if (newValue == null) {
                newValue = new ArrayList<>();
            }
            newValue.add(v);
            return newValue;
        });
    }

    @Override
    public Set<K> keys() {
        return map.keySet();
    }

    @Override
    public Iterator<V> values(K k) {
        return map.get(k).iterator();
    }

    public Map<K, List<V>> asMap() {
        return map;
    }
}
