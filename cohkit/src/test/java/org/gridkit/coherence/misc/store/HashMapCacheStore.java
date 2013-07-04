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

import java.util.HashMap;
import java.util.Map;

import com.tangosol.net.cache.MapCacheStore;

public class HashMapCacheStore extends MapCacheStore {
	public HashMapCacheStore() {
		super(new HashMap<Object, Object>());
	}
	
	@Override
	public void storeAll(@SuppressWarnings("rawtypes") Map mapEntries) {
		super.storeAll(mapEntries);
	}
}
