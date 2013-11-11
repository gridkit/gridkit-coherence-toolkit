package org.gridkit.coherence.example;

import static org.gridkit.coherence.chtest.CacheConfig.distributedSheme;
import static org.gridkit.coherence.chtest.CacheConfig.intantiate;
import static org.gridkit.coherence.chtest.CacheConfig.localScheme;
import static org.gridkit.coherence.chtest.CacheConfig.readWriteBackmingMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.CacheConfig.ReadWriteBackingMap;
import org.gridkit.coherence.chtest.DisposableCohCloud;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.GuardSupport;
import com.tangosol.net.Guardable;
import com.tangosol.net.Guardian.GuardContext;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.CacheLoader;
import com.tangosol.util.WrapperException;

public class SmartGuardianExample {

	@Rule
	public DisposableCohCloud cloud = new DisposableCohCloud();
	
	public static DistributedScheme createSlowScheme() {
		DistributedScheme scheme = distributedSheme();
		ReadWriteBackingMap rwbm = readWriteBackmingMap();
		rwbm.internalCacheScheme(localScheme());
		rwbm.cacheStoreScheme(SlowCacheLoader.class);
		scheme.backingMapScheme(rwbm);
		scheme.partitionCount(1);
		scheme.threadCount(2);
		scheme.guardianTimeout("15s");
		return scheme;
	}
	
	public static DistributedScheme createGuardedSlowScheme() {
		DistributedScheme scheme = distributedSheme();
		ReadWriteBackingMap rwbm = readWriteBackmingMap();
		rwbm.internalCacheScheme(localScheme());
		rwbm.cacheStoreScheme(GuardianAwareCacheLoader.class, intantiate(SlowCacheLoader.class));
		scheme.backingMapScheme(rwbm);
		scheme.partitionCount(1);
		scheme.threadCount(2);
		scheme.guardianTimeout("15s");
		return scheme;		
	}
	
	@Before
	public void initCloud() {
		
		cloud.all().presetFastLocalCluster();
		
		cloud.all()
			.useEmptyCacheConfig()
			.mapCache("unguarded", createSlowScheme())
			.mapCache("guarded", createGuardedSlowScheme());
		
		
		cloud.node("storage").localStorage(true);
		cloud.node("client").localStorage(false);
	}

	@Test(expected = WrapperException.class)
	public void verify_unguarded() {
		
		cloud.node("**").ensureCluster();
		cloud.node("**").getCache("unguarded");
		
		cloud.node("client").exec(new Callable<Void>(){
			@Override
			public Void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("unguarded");
				
				System.out.println("Loading of single key will take just 10 sec");
				cache.get("A");

				System.out.println("But loading of 3 keys should trigger guardian timeout");
				System.out.println("Default thread guardian will intrrupt thread aborting request");
				cache.getAll(Arrays.asList("B", "C", "D"));
				
				
				return null;
			}
		});
	}

	@Test
	public void verify_guarded() {
		
		cloud.node("**").ensureCluster();
		cloud.node("**").getCache("guarded");
		
		cloud.node("client").exec(new Callable<Void>(){
			@Override
			public Void call() throws Exception {
				NamedCache cache = CacheFactory.getCache("guarded");
				
				System.out.println("Loading of single key will take just 10 sec");
				cache.get("A");
				
				System.out.println("But loading of 3 keys should trigger guardian timeout");
				System.out.println("KeyLoaderGuard should prevent request from terminating");
				System.out.println("Guardian will do thread dump anyway though");
				cache.getAll(Arrays.asList("B", "C", "D"));
				
				return null;
			}
		});
	}
	
	
	public static class SlowCacheLoader implements CacheLoader {

		@Override
		public Object load(Object key) {
			System.out.println("Loading key: " + key);
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(10));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			System.out.println("Key loaded: " + key);
			return key;
		}

		@Override
		@SuppressWarnings("rawtypes")
		public Map loadAll(Collection keys) {
			Map<Object, Object> result = new HashMap<Object, Object>();
			for(Object o: keys) {
				result.put(o, load(o));
			}
			return result;
		}
	}

	public static class GuardianAwareCacheLoader implements CacheLoader {
		
		private CacheLoader loader;

		public GuardianAwareCacheLoader(CacheLoader loader) {
			this.loader = loader;
		}

		@Override
		public Object load(Object key) {
			GuardContext ctx = GuardSupport.getThreadContext();
			if (ctx != null) {
				KeyLoaderGuard guard = new KeyLoaderGuard(Collections.singleton(key));
				GuardContext klg = ctx.getGuardian().guard(guard); 
				GuardSupport.setThreadContext(klg);
			}
			try {
				return loader.load(key);
			}
			finally {
				if (ctx != null) {
					GuardContext klg = GuardSupport.getThreadContext();
					GuardSupport.setThreadContext(ctx);
					klg.release();
				}
			}
		}
		
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Map loadAll(Collection keys) {
			GuardContext ctx = GuardSupport.getThreadContext();
			if (ctx != null) {
				KeyLoaderGuard guard = new KeyLoaderGuard(keys);
				GuardContext klg = ctx.getGuardian().guard(guard); 
				GuardSupport.setThreadContext(klg);
				// disable current context
				ctx.heartbeat(TimeUnit.DAYS.toMillis(365));
			}
			try {
				return loader.loadAll(keys);
			}
			finally {
				if (ctx != null) {
					GuardContext klg = GuardSupport.getThreadContext();
					GuardSupport.setThreadContext(ctx);
					klg.release();
					// reenable current context
					ctx.heartbeat();
				}
			}
		}
	}
	
	public static class KeyLoaderGuard implements Guardable {

		Collection<Object> keys;
		GuardContext context;
		
		public KeyLoaderGuard(Collection<Object> keys) {
			this.keys = keys;
		}

		@Override
		public GuardContext getContext() {
			return context;
		}

		@Override
		public void setContext(GuardContext context) {
			this.context = context;
		}
		
		@Override
		public void recover() {
			System.out.println("got RECOVER signal");
			context.heartbeat();
		}

		@Override
		public void terminate() {
			System.out.println("ger TERMINATE signal");
		}

		@Override
		public String toString() {
			return "KeyLoaderGuard:" + keys;
		}
	}
}
