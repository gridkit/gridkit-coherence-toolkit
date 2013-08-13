package org.gridkit.coherence.misc.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import com.tangosol.net.cache.CacheLoader;
import com.tangosol.util.aggregator.ReducerAggregator;

public class ReadThroughWarmUpTest {

	@Rule
	public DisposableCohCloud cloud = new DisposableCohCloud();
	
	@Before
	public void initCloud() {
		
		DistributedScheme scheme = CacheConfig.distributedSheme();
		ReadWriteBackingMap rwbm = CacheConfig.readWriteBackmingMap();
		rwbm.internalCacheScheme(CacheConfig.localScheme());
		rwbm.cacheStoreScheme(TestCacheStore.class);
		scheme.backingMapScheme(rwbm);
		scheme.partitionCount(1);
		
		cloud.all().presetFastLocalCluster();
		cloud.all().mapCache("*", scheme);
		cloud.node("storage").localStorage(true);
		cloud.node("client").localStorage(false);
		cloud.all().getCache("test");		
	}
	
	public static Set<Object> keys(int n) {
		Set<Object> set = new HashSet<Object>();
		for(int i = 0; i < n; ++i) {
			set.add(String.valueOf((char)('A' + i)));
		}
		return set;
	}
	
	@Test
	public void test_get_all() {
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "unchecked", "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("client");
				Map result = cache.getAll(keys(10));
				System.out.println(new TreeMap(result));
				Set<Object> set = new HashSet<Object>(result.values());
				// Due to use of load all number of calls to cache loader should be less than number of keys
				Assert.assertTrue(set.size() < 10);
			}
		});
	}

	@Test
	public void test_reducer_aggregator() {
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "unchecked", "rawtypes" })			
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("client");
				Map result = (Map) cache.aggregate(keys(10), new ReducerAggregator("toString"));
				System.out.println(new TreeMap(result));
				Set<Object> set = new HashSet<Object>(result.values());
				// reducer aggregator have to load keys one dy one 
				Assert.assertTrue(set.size() == 10);
			}
		});
	}

	@Test
	public void test_read_through_warm_upper() {
		cloud.node("client").exec(new VoidCallable() {
			
			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			public void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("client");
				cache.aggregate(keys(10), new ReadThroughWarmUpAggregator());
				Map result = (Map) cache.aggregate(keys(10), new ReducerAggregator("toString"));
				System.out.println(new TreeMap(result));
				Set<Object> set = new HashSet<Object>(result.values());
				// warm up aggregator is exected to load all via one call 
				Assert.assertTrue(set.size() < 10);
			}
		});
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static class TestCacheStore implements CacheLoader {

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
	}
}
