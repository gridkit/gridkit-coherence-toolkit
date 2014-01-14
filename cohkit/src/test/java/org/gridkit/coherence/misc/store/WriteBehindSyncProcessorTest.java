package org.gridkit.coherence.misc.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.gridkit.coherence.chtest.CacheConfig;
import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.CacheConfig.ReadWriteBackingMap;
import org.gridkit.coherence.chtest.DisposableCohCloud;
import org.gridkit.vicluster.VoidCallable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.BinaryEntryStore;
import com.tangosol.net.cache.CacheStore;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;

public class WriteBehindSyncProcessorTest {

	@Rule
	public DisposableCohCloud cloud = new DisposableCohCloud();
	
	@Before
	public void initCloud() {
		
		DistributedScheme scheme = CacheConfig.distributedSheme();
		ReadWriteBackingMap rwbm = CacheConfig.readWriteBackmingMap();
		rwbm.internalCacheScheme(CacheConfig.localScheme());
		rwbm.cacheStoreScheme(TestCacheStore.class);
		rwbm.writeDelay("10s");
		scheme.backingMapScheme(rwbm);
		scheme.partitionCount(3);

		// alternative scheme with BinaryEntryStore
		DistributedScheme bscheme = CacheConfig.distributedSheme();
		ReadWriteBackingMap brwbm = CacheConfig.readWriteBackmingMap();
		brwbm.internalCacheScheme(CacheConfig.localScheme());
		brwbm.cacheStoreScheme(TestEntryStore.class);
		brwbm.writeDelay("10s");
		bscheme.backingMapScheme(brwbm);
		bscheme.partitionCount(3);
		
		cloud.all().presetFastLocalCluster();
		cloud.all().mapCache("*", scheme);
		cloud.all().mapCache("bin-*", bscheme);
		cloud.node("storage").localStorage(true);
		cloud.node("client").localStorage(false);
		cloud.all().getCache("test");		
	}
	
	public static Map<Object, Object> data(int n) {
		Map<Object, Object> map = new HashMap<Object, Object>();
		for(int i = 0; i < n; ++i) {
			map.put(String.valueOf((char)('A' + i)), "" + i);
		}
		return map;
	}
	
	@Test
	public void verify_delayed_write() throws InterruptedException {
		final int n = 10;
		final Map<Object, Object> data = data(n);
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("test");
				cache.putAll(data);
				Assert.assertEquals(n, cache.size());
				Set keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be pending", 0, keys.size());
			}
		});
		
		cloud.node("storage").exec(new VoidCallable() {

			@Override
			public void call() throws Exception {
				Assert.assertEquals(0, BACKING_STORE.size());
			}
		});
		
		System.out.println("Waiting for write-behind");		
		Thread.sleep(15000);

		// Write is expected to be completed by this point
		cloud.node("storage").exec(new VoidCallable() {

			@Override
			public void call() throws Exception {
				Assert.assertEquals(n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});
	}

	@Test
	public void verify_sync_processor() throws InterruptedException {
		final int n = 10;
		final Map<Object, Object> data = data(n);
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("test");
				cache.putAll(data);
				Assert.assertEquals(n, cache.size());
				Set keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be pending", 0, keys.size());
				System.out.println("Invoking sync processor");
				cache.invokeAll(AlwaysFilter.INSTANCE, new WriteBehindSyncProcessor());
			}
		});
		
		cloud.node("storage").exec(new VoidCallable() {
			
			@Override
			public void call() throws Exception {
				Assert.assertEquals("Backing store expected to be updated", n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});
		
		System.out.println("Waiting for write-behind");		
		Thread.sleep(15000);

		// Write is expected to be completed by this point
		cloud.node("storage").exec(new VoidCallable() {

			@Override
			public void call() throws Exception {
				Assert.assertEquals(n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});		
	}

	@Test
	public void verify_sync_processor_with_mark_stored() throws InterruptedException {
		final int n = 10;
		final Map<Object, Object> data = data(n);
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("test");
				cache.putAll(data);
				Assert.assertEquals(n, cache.size());
				Set keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be pending", 0, keys.size());
				System.out.println("Invoking sync processor");
				cache.invokeAll(AlwaysFilter.INSTANCE, new WriteBehindSyncProcessor(true));
				keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be written", n, keys.size());
			}
		});
		
		cloud.node("storage").exec(new VoidCallable() {
			
			@Override
			public void call() throws Exception {
				Assert.assertEquals("Backing store expected to be updated", n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});
		
		System.out.println("Waiting for write-behind");		
		Thread.sleep(15000);
		
		// Write is expected to be completed by this point
		cloud.node("storage").exec(new VoidCallable() {
			
			@Override
			public void call() throws Exception {
				Assert.assertEquals(n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});		
	}

	@Test
	public void verify_sync_processor_with_entry_store() throws InterruptedException {
		final int n = 10;
		final Map<Object, Object> data = data(n);
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("bin-test");
				cache.putAll(data);
				Assert.assertEquals(n, cache.size());
				Set keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be pending", 0, keys.size());
				System.out.println("Invoking sync processor");
				cache.invokeAll(AlwaysFilter.INSTANCE, new WriteBehindSyncProcessor(true));
				keys = cache.keySet(new EqualsFilter(StoreFlagExtractor.INSTANCE, Boolean.TRUE));
				Assert.assertEquals("All keys are expected to be written", n, keys.size());
			}
		});
		
		cloud.node("storage").exec(new VoidCallable() {
			
			@Override
			public void call() throws Exception {
				Assert.assertEquals("Backing store expected to be updated", n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});
		
		System.out.println("Waiting for write-behind");
		Thread.sleep(15000);
		
		// Write is expected to be completed by this point
		cloud.node("storage").exec(new VoidCallable() {
			
			@Override
			public void call() throws Exception {
				Assert.assertEquals(n, BACKING_STORE.size());
				Assert.assertEquals(data.keySet(), BACKING_STORE.keySet());
			}
		});		
	}
	
	private static Map<Object, Object> BACKING_STORE = new ConcurrentHashMap<Object, Object>();
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static class TestCacheStore implements CacheStore {

		private AtomicInteger counter = new AtomicInteger();
		
		@Override
		public Object load(Object k) {
			System.out.println("load: " + k);
			return "call" + counter.incrementAndGet();
		}

		@Override
		public Map loadAll(Collection keys) {
			System.out.println("loadAll: " + keys);
			int i = counter.incrementAndGet();
			Map result = new HashMap(keys.size());
			for(Object k: keys) {
				result.put(k, "call" + i);
			}
			return result;
		}

		@Override
		public void erase(Object k) {
			System.out.println("erase: " + k);
		}

		@Override
		public void eraseAll(Collection keys) {
			System.out.println("eraseAll: " + keys);			
		}

		@Override
		public void store(Object k, Object v) {
			System.out.println("store-start: " + k + " -> " + v);			
			BACKING_STORE.put(k, v);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			    // ignore
			}
			System.out.println("store-finish: " + k + " -> " + v);			
		}

		@Override
		public void storeAll(Map entries) {
            System.out.println("store-start: " + entries);           
            BACKING_STORE.putAll(entries);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
            System.out.println("store-finish: " + entries);          
		}
	}

	@SuppressWarnings({"rawtypes" })
	public static class TestEntryStore implements BinaryEntryStore {
		
		@Override
		public void erase(BinaryEntry entry) {
		}

		@Override
		public void eraseAll(Set entry) {
		}

		@Override
		public void load(BinaryEntry entry) {
		}

		@Override
		public void loadAll(Set entries) {
		}

		@Override
		public void store(BinaryEntry entry) {
			storeAll(Collections.singleton(entry));
		}

		@Override
		public void storeAll(Set entries) {
			for(Object o: entries) {
				BinaryEntry be = (BinaryEntry) o;
				BACKING_STORE.put(be.getKey(), be.getValue());
			}			
		}
	}
}
