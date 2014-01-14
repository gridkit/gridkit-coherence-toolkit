/**
 * Copyright 2013 Alexey Ragozin
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.cache.BinaryEntryStore;
import com.tangosol.net.cache.CacheStore;
import com.tangosol.net.cache.ReadWriteBackingMap;
import com.tangosol.net.cache.ReadWriteBackingMap.BinaryEntryStoreWrapper;
import com.tangosol.net.cache.ReadWriteBackingMap.CacheStoreWrapper;
import com.tangosol.net.cache.ReadWriteBackingMap.StoreWrapper;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * This entry processor collects all entries not-yet-stored by write-behind
 * and stores them synchronously.
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class WriteBehindSyncProcessor extends AbstractProcessor implements PortableObject {

	private static final long serialVersionUID = 20140114L;

	private boolean markAsStored = false;
	
	public WriteBehindSyncProcessor() {		
	}

	/**
	 * @param markAsStored if <code>true</code> write pending flag would be remove from entries (though they will not be deleted from write-behind queue) 
	 */
	public WriteBehindSyncProcessor(boolean markAsStored) {
		this.markAsStored = markAsStored;
	}
	
	@Override
	public Object process(Entry entry) {
		processAll(Collections.singleton(entry));
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Map processAll(Set entries) {

		StoreFlagExtractor extractor = StoreFlagExtractor.INSTANCE;
		Map internalMap = null;
		CacheStore objectStore = null;
		BinaryEntryStore entryStore = null;
		
		
		Set<BinaryEntry> candidates = new HashSet<BinaryEntry>();
        for(Object e: entries) {
            BinaryEntry entry = (BinaryEntry) e;
            
            if (objectStore == null && entryStore == null) {
            	ReadWriteBackingMap rwmap = (ReadWriteBackingMap) entry.getBackingMap();
            	internalMap = rwmap.getInternalCache();
                StoreWrapper stw = rwmap.getCacheStore();
                if (stw instanceof CacheStoreWrapper) {
                	objectStore = ((CacheStoreWrapper) stw).getCacheStore();
                }
                else {
                	entryStore = ((BinaryEntryStoreWrapper) stw).getBinaryEntryStore();
                }
            }

            if (Boolean.FALSE.equals(extractor.extractFromEntry(entry))) {
            	// this entry is not stored yet
            	candidates.add(entry);
            }
        }

        if (!candidates.isEmpty()) {
        	if (objectStore != null) {
        		Map<Object, Object> data = new HashMap<Object, Object>();
        		for(BinaryEntry be: candidates) {
        			data.put(be.getKey(), be.getValue());
        		}
        		objectStore.storeAll(data);        		
        	}
        	else {
        		entryStore.storeAll(candidates);
        	}
        	if (markAsStored) {
        		for(BinaryEntry be: candidates) {
        			Binary key = be.getBinaryKey();
        			Binary value = be.getBinaryValue();
        			value = ExternalizableHelper.undecorate(value, ExternalizableHelper.DECO_STORE);
        			internalMap.put(key, value);
        		}
        	}
        }
        
        return Collections.emptyMap();
	}

	@Override
	public void readExternal(PofReader pofIn) throws IOException {
		markAsStored = pofIn.readBoolean(1);
	}

	@Override
	public void writeExternal(PofWriter pofOut) throws IOException {
		pofOut.writeBoolean(1, markAsStored);
	}
}
