package org.gridkit.coherence.misc.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.gridkit.coherence.chtest.CacheConfig;
import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.DisposableCohCloud;
import org.junit.Rule;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.WrapperException;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.extractor.ReflectionExtractor;

public class IndexPresenceFilterTest {
	
	@Rule
	public static DisposableCohCloud cloud = new DisposableCohCloud() {

		@Override
		protected void before() throws Throwable {
			super.before();			
			DistributedScheme scheme = CacheConfig.distributedSheme();
			scheme.backingMapScheme(CacheConfig.localScheme());
			
			all().presetFastLocalCluster();
			all().mapCache("*", scheme);
			node("client*").localStorage(false);
			node("storage*").localStorage(true);
			node("client");
			node("storage-1");
			node("storage-2");
		}
	};
	
	@Test
	public void test_data_check() {
		final String cacheName = "test_data_check";
		
		cloud.all().getCache(cacheName);
		NamedCache cache = cloud.node("client").getCache(cacheName);
		generateData(cache, 1000, 4, 10);
		
		cloud.node("client").exec(new Runnable() {
			
			@Override
			public void run() {
				NamedCache cache = CacheFactory.getCache(cacheName);
				ReflectionExtractor ve = new ReflectionExtractor("getFieldA");
				cache.addIndex(ve, false, null);
				
				int count = ((Number)cache.aggregate(new IndexPresenceFilter(ve, "0"), new Count())).intValue();
				
				Assert.assertTrue("Count should be greater than 0", count > 0);

				count = ((Number)cache.aggregate(new IndexPresenceFilter(ve, "4"), new Count())).intValue();
				
				Assert.assertTrue("Count should be 0", count == 0);				
			}
		});
	}

	@Test(expected = WrapperException.class)
	public void test_no_index_failure() {
		final String cacheName = "test_no_index_failure";
		
		cloud.all().getCache(cacheName);
		NamedCache cache = cloud.node("client").getCache(cacheName);
		generateData(cache, 1000, 4, 10);
		
		cloud.node("client").exec(new Runnable() {
			
			@Override
			public void run() {
				NamedCache cache = CacheFactory.getCache(cacheName);
				ReflectionExtractor ve = new ReflectionExtractor("getFieldA");
				
				cache.aggregate(new IndexPresenceFilter(ve, "0"), new Count());
				
				Assert.fail("Expection expected");
			}
		});
	}

	private void generateData(NamedCache cache, int total, int modA, int modB) {
		Map<Integer, Data> data = new HashMap<Integer, IndexPresenceFilterTest.Data>();
		for(int i = 0; i != total; ++i) {
			Data d = new Data(String.valueOf(i % modA), String.valueOf(i % modB));
			data.put(i, d);
		}
		cache.putAll(data);
	}
	
	
	@SuppressWarnings("serial")
	public static class Data implements Serializable {
		
		public String fieldA;
		public String fieldB;
		
		public Data(String fieldA, String fieldB) {
			this.fieldA = fieldA;
			this.fieldB = fieldB;
		}

		public String getFieldA() {
			return fieldA;
		}

		public String getFieldB() {
			return fieldB;
		}
	}
}
