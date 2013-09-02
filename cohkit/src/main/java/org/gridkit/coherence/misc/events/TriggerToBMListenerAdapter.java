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
package org.gridkit.coherence.misc.events;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.util.Base;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.MapTrigger;
import com.tangosol.util.MapTriggerListener;
import com.tangosol.util.filter.FilterTrigger;

/**
 * Using this class you can let existing backing map listeners 
 * to be executed in map trigger context.  
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public abstract class TriggerToBMListenerAdapter extends MapTriggerListener implements MapTrigger, PortableObject, Externalizable {

	private static final long serialVersionUID = 20121214L;
	
	private transient volatile MapListener listener;

	protected abstract MapListener instantiateListener(String cacheName, BackingMapManagerContext context);
	
	public TriggerToBMListenerAdapter() {
		// have to pass something to constructor
		// this trigger will never be used
		super(new FilterTrigger());
	}
	
	@Override
	public MapTrigger getTrigger() {
		return this;
	}

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return true;
	}

	@Override
	public void readExternal(PofReader in) throws IOException {
	}

	@Override
	public void writeExternal(PofWriter out) throws IOException {
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// no fields persisted
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// no fields persisted
	}

	@Override
	public void process(Entry entry) {
		try {
			BinaryEntry be = ((BinaryEntry)entry);
			if (listener == null) {
				synchronized (this) {
					if (listener == null) {
						String cacheName = be.getBackingMapContext().getCacheName();
						listener = instantiateListener(cacheName, be.getContext());
					}
				}
			}
			dispatch(listener, be);
		}
		catch(Exception e) {
			CacheFactory.log("TriggerToBMListenerAdapter: " + e.toString(), Base.LOG_WARN);
		}
	}

	private void dispatch(MapListener listener, BinaryEntry entry) {
		if (entry.getOriginalBinaryValue() == null) {
			MapEvent me = new MapEvent(entry.getBackingMap(), MapEvent.ENTRY_INSERTED, entry.getBinaryKey(), entry.getOriginalBinaryValue(), entry.getBinaryValue());
			listener.entryInserted(me);
		}
		else if (entry.getBinaryValue() == null) {
			MapEvent me = new MapEvent(entry.getBackingMap(), MapEvent.ENTRY_DELETED, entry.getBinaryKey(), entry.getOriginalBinaryValue(), entry.getBinaryValue());
			listener.entryDeleted(me);
		}
		else {
			MapEvent me = new MapEvent(entry.getBackingMap(), MapEvent.ENTRY_UPDATED, entry.getBinaryKey(), entry.getOriginalBinaryValue(), entry.getBinaryValue());
			listener.entryUpdated(me);
		}
	}
}
