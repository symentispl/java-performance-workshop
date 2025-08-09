package pl.symentis.mapreduce.mapper;

import java.util.Iterator;
import java.util.Set;
import org.eclipse.collections.impl.multimap.list.FastListMultimap;
import pl.symentis.mapreduce.core.MapperOutput;

public final class EclipseCollectionsMultiMapOutput<K, V> implements MapperOutput<K, V> {

    private final org.eclipse.collections.api.multimap.MutableMultimap<K, V> multimap = FastListMultimap.newMultimap();

    @Override
    public void emit(K k, V v) {
        multimap.put(k, v);
    }

    @Override
    public Set<K> keys() {
        return multimap.keySet().toSet();
    }

    @Override
    public Iterator<V> values(K k) {
        return multimap.get(k).iterator();
    }
}
