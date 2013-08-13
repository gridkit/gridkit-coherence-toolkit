package org.gridkit.coherence.misc.store;

import java.util.HashSet;
import java.util.Set;

import com.tangosol.net.cache.CacheMap;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.aggregator.Count;

public class ReadThroughWarmUpAggregator extends Count {

	private static final long serialVersionUID = 20130813L;

	public static void preloadValuesViaReadThrough(Set<BinaryEntry> entries) {
        CacheMap backingMap = null;
        Set<Object> keys = new HashSet<Object>();
        for (BinaryEntry entry : entries) {
            if (backingMap == null) {
                backingMap = (CacheMap) entry.getBackingMapContext().getBackingMap();
            }
            if (!entry.isPresent()) {
                keys.add(entry.getBinaryKey());
            }
        }
        backingMap.getAll(keys);
    }	
	
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object aggregate(Set entries) {
		preloadValuesViaReadThrough(entries);		
		return super.aggregate(entries);
	}
}
