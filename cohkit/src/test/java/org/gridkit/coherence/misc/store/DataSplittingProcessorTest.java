package org.gridkit.coherence.misc.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import junit.framework.Assert;

import org.gridkit.coherence.chtest.CacheConfig;
import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.DisposableCohCloud;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.CompositeKey;


public class DataSplittingProcessorTest {

	@Rule
	public  DisposableCohCloud cloud = new DisposableCohCloud();
	
	@Before
	public void initCloud() {
		cloud.all().presetFastLocalCluster();
		
		DistributedScheme scheme = CacheConfig.distributedSheme();
		scheme.backingMapScheme(CacheConfig.localScheme());
		scheme.partitionCount(2);
		scheme.autoStart(true);
		
		cloud.all().useEmptyCacheConfig().mapCache("*", scheme);
		cloud.node("storage*").localStorage(true);
		cloud.node("client*").localStorage(false);
	}

	@Test
	public void test_simple_key_processing() throws InterruptedException {

		cloud.nodes("client", "server1").startCacheServer();
		NamedCache cache = cloud.node("client").getCache("test");
		
		
		
		cache.put("A", "A");
		cache.put("B", "B");
		cache.put("C", "C");
		
		final Map<String, String> data = new HashMap<String, String>();
		data.put("A", "0");
		data.put("B", "1");
		data.put("C", "2");
		data.put("D", "3");
		
		cloud.nodes("client").exec(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("test");
				DataSplittingProcessor.invoke(cache, data, new AppendProcessor());
				return null;
			}
		});
		
		Assert.assertEquals("A0", cache.get("A"));
		Assert.assertEquals("B1", cache.get("B"));
		Assert.assertEquals("C2", cache.get("C"));
		Assert.assertEquals("3", cache.get("D"));
	}

	private CompositeKey ck(Object p, Object s) {
		return new CompositeKey(p, s);
	}
	
	@Test
	public void test_composite_key_processing() {
		
		cloud.nodes("client", "server1", "server2").startCacheServer();
		NamedCache cache = cloud.node("client").getCache("test");
		
		cache.put(ck("A", "A"), "A");
		cache.put(ck("B", "B"), "B");
		cache.put(ck("B", "C"), "C");
		
		final Map<Object, Object> data = new HashMap<Object, Object>();
		data.put(ck("A", "A"), "0");
		data.put(ck("B", "B"), "1");
		data.put(ck("B", "C"), "2");
		data.put(ck("B", "D"), "3");
		
		cloud.nodes("client").exec(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("test");
				DataSplittingProcessor.invoke(cache, data, new AppendProcessor());
				return null;
			}
		});
		
		Assert.assertEquals("A0", cache.get(ck("A", "A")));
		Assert.assertEquals("B1", cache.get(ck("B", "B")));
		Assert.assertEquals("C2", cache.get(ck("B", "C")));
		Assert.assertEquals("3", cache.get(ck("B", "D")));
	}
	
	@SuppressWarnings("serial")
	public static class AppendProcessor implements EntryDataProcessor, Serializable {
		@Override
		public Object process(EntryWithData entry) {
			System.out.println("Appending to " + entry.getEntry().getBinaryKey());
			String val = (String) entry.getEntry().getValue();
			val = val == null ? "" : val;
			val += entry.getPayloadData();
			entry.getEntry().setValue(val, false);
			return null;
		}
	}
}
