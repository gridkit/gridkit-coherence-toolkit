/**
 * Copyright 2012-2013 Alexey Ragozin
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tangosol.net.cache.ReadWriteBackingMap;
import com.tangosol.net.cache.ReadWriteBackingMap.CacheStoreWrapper;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

public class BatchStoreProcessor extends AbstractProcessor {
	private static final long serialVersionUID = 1L;
	
	private Map<Binary, Binary> payload;
	
	public BatchStoreProcessor(Map<Binary, Binary> payload) {
		this.payload = payload;
	}

	@Override
	public Object process(Entry entry) {
		processAll(Collections.singleton(entry));
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Map processAll(Set entries) {
		Map backingMap = null;
		Map<Binary, Binary> candidates = new HashMap<Binary, Binary>(entries.size());
        for(Object e: entries) {
            BinaryEntry entry = (BinaryEntry) e;
            
            if (backingMap == null) {
                backingMap = entry.getBackingMap();
            }
            
            Binary candidateValue = payload.get(entry.getBinaryKey());
            if (candidateValue != null) {
            	candidates.put(entry.getBinaryKey(), candidateValue);
            }
        }

        if (!candidates.isEmpty()) {
	        ReadWriteBackingMap rwmap = (ReadWriteBackingMap) backingMap;
	        ((CacheStoreWrapper)rwmap.getCacheStore()).getCacheStore().storeAll(candidates);
	        Map missesCache = rwmap.getMissesCache();
	        if (missesCache != null) {
		        for (Binary key : candidates.keySet()) {
		        	missesCache.remove(key);
		        }
	        }
	        rwmap.getInternalCache().putAll(candidates);
        }
        
        return Collections.emptyMap();
	}
}
