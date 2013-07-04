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

import com.tangosol.net.cache.LocalCache;
import com.tangosol.util.MapListener;

@SuppressWarnings("serial")
public class ProcessingCacheMap extends LocalCache {

	private MapListener listener;
	
	private ProcessingCacheMap() {
		super();
	}
	
	@Override
	@SuppressWarnings("deprecation")
	public synchronized void addMapListener(MapListener listener) {
		super.addMapListener(listener);
		System.out.println("Listener added: " + listener);
		this.listener = listener;
	}
	
	@Override
	@SuppressWarnings("deprecation")
	public Object put(Object oKey, Object oValue) {
		System.out.println("PUT at " + oKey + ", listener: " + listener);
		return super.put(oKey, oValue);
	}
}
