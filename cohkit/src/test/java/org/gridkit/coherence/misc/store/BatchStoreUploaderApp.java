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

import org.gridkit.coherence.misc.store.BatchStoreUploader;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

public class BatchStoreUploaderApp {
	public static void main(String[] args) {
		System.setProperty("tangosol.coherence.cacheconfig", "batch-uploader-test-cache-config.xml");
		System.setProperty("tangosol.coherence.localhost", "127.0.0.1");
		
		NamedCache cache = CacheFactory.getCache("store-cache");
		
		BatchStoreUploader uploader = new BatchStoreUploader(cache);
		uploader.put("1", "test");
		uploader.put("2", "test2");
		uploader.flush();
		
		System.out.println(cache.get("1"));
		System.out.println(cache.get("2"));
	}
}
