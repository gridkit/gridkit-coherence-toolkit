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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gridkit.coherence.misc.store.EntryDataProcessor.EntryWithData;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.KeyAssociation;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.CompositeKey;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

public class DataSplittingProcessor {

	public static Map<?, ?> invoke(NamedCache cache, Map<?, ?> keysAndData, EntryDataProcessor processor) {
		if (keysAndData.isEmpty()) {
			return Collections.emptyMap();
		}
		if (processor == null) {
			throw new NullPointerException("processor is null");
		}
		if (cache == null) {
			throw new NullPointerException("cache is null");
		}
		boolean keyAssociated = keysAndData.keySet().iterator().next() instanceof KeyAssociation;
		if (keyAssociated) {
			List<Object> set = new ArrayList<Object>(keysAndData.size());
			for(Map.Entry<?, ?> e: keysAndData.entrySet()) {
				Object assoc = ((KeyAssociation)e.getKey()).getAssociatedKey();
				CompositeKey capsule = new CompositeKey(e.getKey(), e.getValue());
				CompositeKey key = new CompositeKey(assoc, capsule);
				set.add(key);
			}
			return cache.invokeAll(set, new KeyAssociationAwareProcessor(processor));
		}
		else {
			List<Object> set = new ArrayList<Object>(keysAndData.size());
			for(Map.Entry<?, ?> e: keysAndData.entrySet()) {
				CompositeKey key = new CompositeKey(e.getKey(), e.getValue());
				set.add(key);
			}
			return cache.invokeAll(set, new SimpleProcessor(processor));			
		}
	}
	
	public static class SimpleProcessor extends AbstractProcessor implements Serializable, PortableObject {
		
		private static final long serialVersionUID = 20130817L;		
		
		private EntryDataProcessor processor; 
		
		/**
		 * @deprecated Internal constructor for POF support
		 */
		public SimpleProcessor() {			
		}

		protected SimpleProcessor(EntryDataProcessor processor) {
			this.processor = processor;
		}

		@Override
		@SuppressWarnings("rawtypes")
		public Map processAll(Set entries) {
			return super.processAll(sortEntries(entries));
		}

		@Override
		public Object process(Entry entry) {
			if (entry.isPresent()) {
				throw new IllegalStateException("DataSplittingProcessor is invoked in wrong way");
			}
			BinaryEntry bentry = (BinaryEntry) entry;
			CompositeKey ckey = (CompositeKey) entry.getKey();
			Object key = ckey.getPrimaryKey();
			Object data = ckey.getSecondaryKey();
			
			return processor.process(new DataEntry(bentry, key, data));
		}
		
		@Override
		public void readExternal(PofReader in) throws IOException {
			processor = (EntryDataProcessor) in.readObject(1);
		}

		@Override
		public void writeExternal(PofWriter out) throws IOException {
			out.writeObject(1, processor);
		}				
	}

	public static class KeyAssociationAwareProcessor extends AbstractProcessor implements Serializable, PortableObject {

		private static final long serialVersionUID = 20130817L;
		
		private EntryDataProcessor processor; 

		/**
		 * @deprecated Internal constructor for POF support
		 */
		public KeyAssociationAwareProcessor() {
		}

		protected KeyAssociationAwareProcessor(EntryDataProcessor processor) {
			this.processor = processor;
		}

		@Override
		@SuppressWarnings("rawtypes")
		public Map processAll(Set entries) {
			return super.processAll(sortEntries(entries));
		}

		@Override
		public Object process(Entry entry) {
			if (entry.isPresent()) {
				throw new IllegalStateException("DataSplittingProcessor is invoked in wrong way");
			}
			BinaryEntry bentry = (BinaryEntry) entry;
			CompositeKey ckey = (CompositeKey) entry.getKey();
			CompositeKey capsule = (CompositeKey) ckey.getSecondaryKey();

			Object key = capsule.getPrimaryKey();
			Object data = capsule.getSecondaryKey();
			
			return processor.process(new DataEntry(bentry, key, data));
		}

		@Override
		public void readExternal(PofReader in) throws IOException {
			processor = (EntryDataProcessor) in.readObject(1);
		}

		@Override
		public void writeExternal(PofWriter out) throws IOException {
			out.writeObject(1, processor);
		}		
	}	
	
	private static class DataEntry implements EntryWithData {
		
		private BinaryEntry entry;
		private Object data;
		
		public DataEntry(BinaryEntry ientry, Object key, Object data) {
			Binary bkey = (Binary) ientry.getContext().getKeyToInternalConverter().convert(key);
			this.data = entry = (BinaryEntry) ientry.getBackingMapContext().getBackingMapEntry(bkey);
			this.data = data;
		}
		
		public BinaryEntry getEntry() {
			return entry;
		}
		
		public Object getPayloadData() {
			return data;
		}		
	}
	
	/**
	 * Sorting of entries should prevent deadlock over keys between
	 * concurrent executions.
	 * @param entries
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Set sortEntries(Set entries) {
		ArraySet sortedSet = new ArraySet(entries);
		Collections.sort(sortedSet, BKC);
		return sortedSet;
	}
	
	@SuppressWarnings({ "rawtypes", "serial" })
	private static class ArraySet extends ArrayList implements Set {
		
		@SuppressWarnings("unchecked")
		public ArraySet(Collection c) {
			super(c);
		}
	}
	
	private static BinaryKeyComparator BKC = new BinaryKeyComparator();
	
	private static class BinaryKeyComparator implements Comparator<BinaryEntry> {

		@Override
		public int compare(BinaryEntry o1, BinaryEntry o2) {
			return o1.getBinaryKey().compareTo(o2.getBinaryKey());
		}		
	}
}
