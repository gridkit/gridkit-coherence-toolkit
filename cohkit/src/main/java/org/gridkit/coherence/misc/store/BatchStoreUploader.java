/**
 * Copyright 2012 Dmitri Babaev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gridkit.coherence.misc.store;

import java.util.HashMap;
import java.util.Map;

import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.util.Binary;
import com.tangosol.util.Converter;
import com.tangosol.util.UID;

/**
 * A tool to upload a bulk of entries to the cache with synchronous write to the underlying storage.
 * This is done by issuing a set of per-node entry processors which performs a per-partition batch put operation directly to the backing map.
 * This will work for for write-through backing maps.
 *
 * @author Dmitri Babaev
 */
public class BatchStoreUploader {
	private int perMemberPutBatchSize;

	private NamedCache cache;
    
    private Map<UID, Map<Object, Object>> nodeBuffer = new HashMap<UID, Map<Object, Object>>();
    
    public BatchStoreUploader(NamedCache cache) {
    	this(cache, 32);
    }

    public BatchStoreUploader(NamedCache cache, int perNodePutBatchSize) {
    	this.cache = cache;
    	this.perMemberPutBatchSize = perNodePutBatchSize;
    }
    
	private void push(Map<Object, Object> batch) {
        Converter keyConverter = cache.getCacheService().getBackingMapManager().getContext().getKeyToInternalConverter();
        Converter valueConverter = cache.getCacheService().getBackingMapManager().getContext().getValueToInternalConverter();
        Map<Binary, Binary> binMap = new HashMap<Binary, Binary>(batch.size());
        for(Map.Entry<Object, Object> entry: batch.entrySet()) {
                Binary key = (Binary) keyConverter.convert(entry.getKey());
                Binary value = (Binary) valueConverter.convert(entry.getValue());
                binMap.put(key, value);
        };
        cache.invokeAll(batch.keySet(), new BatchStoreProcessor(binMap));
	}

	public void put(Object key, Object value) {
	        Member member = ((PartitionedService)cache.getCacheService()).getKeyOwner(key);
	        UID id = member.getUid();
	        Map<Object, Object> buf = nodeBuffer.get(id);
	        if (buf == null) {
	                buf = new HashMap<Object, Object>();
	                nodeBuffer.put(id, buf);
	        }
	        buf.put(key, value);
	        if (buf.size() >= perMemberPutBatchSize) {
	        	push(buf);
                buf.clear();
	        }
	}
	
	public <K, V> void putAll(Map<K, V> map) {
	        for(Map.Entry<K, V> entry: map.entrySet()) {
	                put(entry.getKey(), entry.getValue());
	        }
	}
	
	public void flush() {
		for (Map<Object, Object> buf : nodeBuffer.values()) {
	        push(buf);
	        buf.clear();
		}
	}
}
