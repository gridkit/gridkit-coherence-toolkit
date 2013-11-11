package org.gridkit.coherence.example;

import static org.gridkit.coherence.chtest.CacheConfig.distributedSheme;
import static org.gridkit.coherence.chtest.CacheConfig.intantiate;
import static org.gridkit.coherence.chtest.CacheConfig.localScheme;
import static org.gridkit.coherence.chtest.CacheConfig.readWriteBackmingMap;

import java.util.Set;
import java.util.concurrent.Callable;

import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.CacheConfig.ReadWriteBackingMap;
import org.gridkit.coherence.chtest.CohCloud.CohNode;
import org.gridkit.coherence.chtest.DisposableCohCloud;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.BinaryEntryStore;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapTriggerListener;
import com.tangosol.util.processor.AbstractProcessor;
import com.tangosol.util.processor.ExtractorProcessor;

public class EraseElimitationExample {

	@Rule
	public DisposableCohCloud cloud = new DisposableCohCloud();
	
	@Before
	public void initCloud() {
		
		DistributedScheme scheme = distributedSheme();
		ReadWriteBackingMap rwbm = readWriteBackmingMap();
		rwbm.internalCacheScheme(localScheme());
		rwbm.cacheStoreScheme(EraseOptimizerStoreWrapper.class, intantiate(PrintBinaryEntryStrore.class));
		rwbm.writeDelay("2s");
		scheme.backingMapScheme(rwbm);
		scheme.listener(MapTriggerListener.class, intantiate(NewDecoratingTrigger.class));
		
		cloud.all().presetFastLocalCluster();
		
		cloud.all()
			.useEmptyCacheConfig()
			.mapCache("*", scheme);
		
		
		cloud.node("storage1").localStorage(true);
		cloud.node("storage2").localStorage(true);
		cloud.node("client").localStorage(false);
	}
	
	@Test
	public void test() {

		final String cacheName  = "test";	
		cloud.all().ensureCluster();
		cloud.node("storage*").getCache(cacheName);
		
		CohNode client = cloud.node("client");
		client.exec(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				NamedCache cache = CacheFactory.getCache(cacheName);
				
				System.out.println("Updating cache and removing immediately, cache store should not be involved");
				System.out.println("No STORE and ERASE should happen (ok)");

				cache.put("A", "1");
				cache.put("A", "2");
				cache.put("A", "3");
				cache.remove("A");

				Thread.sleep(5000);

				System.out.println("Updating cache and removing slowly, cache store should store and put");
				System.out.println("STORE then ERASE is expected (broken)");
				
				cache.put("B", "1");
				Thread.sleep(700);
				cache.put("B", "2");
				Thread.sleep(700);
				cache.put("B", "3");
				Thread.sleep(700);
				System.out.println("B stored: " + cache.invoke("B", new ExtractorProcessor(new StoreFlagExtractor())));				
				cache.remove("B");

				Thread.sleep(2000);

				System.out.println("Loading entry via read through and removing");
				System.out.println("LOAD then ERASE is expected (ok)");
							
				cache.get("C");
				cache.remove("C");

				System.out.println("Test EP read-though");
				
				cache.invoke("X", new ReadThroughEntry());
				
				Thread.sleep(2000);

				System.out.println("Updating cache very slow to ensure race");
				System.out.println("multiple STORE then ERASE is expected (broken)");
				
				cache.put("D", "1");
				Thread.sleep(1200);
				cache.put("D", "2");
				Thread.sleep(1200);
				cache.put("D", "3");
				Thread.sleep(1200);
				cache.put("D", "4");
				Thread.sleep(1200);
				cache.put("D", "5");
				Thread.sleep(1200);
				cache.put("D", "6");
				Thread.sleep(1200);
				System.out.println("D stored: " + cache.invoke("D", new ExtractorProcessor(new StoreFlagExtractor())));
				cache.remove("D");

				Thread.sleep(2000);

				
				return null;
			}
		});
		
	}
	
	@SuppressWarnings("serial")
	public static class ReadThroughEntry extends AbstractProcessor {

		@Override
		public Object process(Entry e) {
			System.out.println("[" + e.getKey() + "].isPresent = " + e.isPresent());
			e.getValue();
			return null;
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	public static class PrintBinaryEntryStrore implements BinaryEntryStore {

		@Override
		public void erase(BinaryEntry entry) {
			System.out.println("ERASE: " + entry.getKey());
		}

		@Override
		public void eraseAll(Set entries) {
			for(Object e: entries) {
				erase((BinaryEntry) e);
			}			
		}

		@Override
		public void load(BinaryEntry entry) {			
			System.out.println("LOAD: " + entry.getKey());
			entry.setValue("Loaded");
		}

		@Override
		public void loadAll(Set entries) {
			for(Object e: entries) {
				load((BinaryEntry) e);
			}			
		}

		@Override
		public void store(BinaryEntry entry) {
			System.out.println("STORE: " + entry.getKey() + " -> " + entry.getValue());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println("STORE - finished");
		}

		@Override
		public void storeAll(Set entries) {
			for(Object e: entries) {
				store((BinaryEntry) e);
			}			
		}
	}
}
