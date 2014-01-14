package org.gridkit.coherence.misc.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.gridkit.coherence.chtest.CacheConfig;
import org.gridkit.coherence.chtest.CohCloud;
import org.gridkit.coherence.chtest.CacheConfig.DistributedScheme;
import org.gridkit.coherence.chtest.CohCloud.CohNode;
import org.gridkit.coherence.chtest.SimpleCohCloud;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;

public class GradualIteratorTest {
    private CohCloud cloud;
    
    @After
    public void after() {
        if (cloud != null) {
            cloud.shutdown();
        }
    }
    
    public void newCloud(int nStorage, int partitionCount) {
        cloud = new SimpleCohCloud();
                
        proxy().localStorage(false);
        for (int i = 0; i < nStorage; ++i) {
            cloud.node("storage-" + i).localStorage(true);
        }
        
        DistributedScheme scheme = CacheConfig.distributedSheme();
        scheme.serviceName("GradualService");
        scheme.autoStart(true);
        scheme.backingMapScheme(CacheConfig.localScheme());
        scheme.partitionCount(partitionCount);
        
        all().presetFastLocalCluster();
        all().mapCache("gradual-cache", scheme);
        all().startCacheServer();
    }
    
    @Test
    public void test_one_storage() {
        newCloud(1, 17);
        test(256, 1);
        test(256, 2);
    }

    @Test
    public void test_two_storage() {
        newCloud(2, 17);
        test(256, 1);
        test(256, 2);
        test(256, 5);
    }
    
    public void test(int limit, int factor) {
        // result set less that limit
        test(0, limit, factor);
        test(1, limit, factor);
        test(2, limit, factor);
        test(8, limit, factor);
        test(limit / 2, limit, factor);
        test(limit - 1, limit, factor);
        
        // result set size = limit + portion of limit
        test(limit, limit, factor);
        test(limit + 1, limit, factor);
        test(limit + 2, limit, factor);
        test(limit + 8, limit, factor);
        test(limit + limit / 2, limit, factor);
        test(2 * limit - 1, limit, factor);
        
        // result set size = 2 * limit + portion of limit
        test(2 * limit, limit, factor);
        test(2 * limit + 1, limit, factor);
        test(2 * limit + 2, limit, factor);
        test(2 * limit + 8, limit, factor);
        test(2 * limit + limit / 2, limit, factor);
        test(3 * limit - 1, limit, factor);
        
        // result set size = 3 * limit
        test(3 * limit, limit, factor);
    }

    public void test(int size, int limit, int factor) {
        Tester tester = new Tester();
        tester.size = size;
        tester.limit = limit;
        tester.factor = factor;
        
        proxy().exec(tester);
    }
    
    @SuppressWarnings("serial")
    public static class Tester implements Runnable, Serializable {
        public int size;
        public int limit;
        public int factor;
        
        @Override
        public void run() {
            Map<String, Integer> expectedEntries = generate(size, factor);
            
            NamedCache cache = CacheFactory.getCache("gradual-cache");
            cache.clear();
            cache.putAll(expectedEntries);
              
            int partitionCount = ((PartitionedService)cache.getCacheService()).getPartitionCount();
            
            GradualIterator iter = new GradualIterator(cache, partitionCount);
            iter.setLimit(limit / partitionCount);
            iter.setClearValue(false);
            
            Map<String, Integer> resultEntries = new LinkedHashMap<String, Integer>();
            
            List<Integer> sizes = new ArrayList<Integer>();
            
            while (iter.hasNext()) {
                List<GradualIterator.Entry> next = iter.getNext();
                for (GradualIterator.Entry entry : next) {
                    resultEntries.put(entry.<String>getKey(), entry.<Integer>getValue());
                }
                sizes.add(next.size());
            }
            
            List<?> expectedList = toList(expectedEntries);
            List<?> resultList = new ArrayList<Map.Entry<String, Integer>>(resultEntries.entrySet());
            
            Assert.assertEquals(expectedList, resultList);
        }
    }
    
    public static Map<String, Integer> generate(int size, int factor) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        
        int key = 0;
        for (int i = 0; i < size; ++i) {
            int mirror = size - i - 1;
            for (int f = 0; f < factor; ++f) {
                result.put(String.format("%08d", key++), mirror);
            }
        }
        
        return result;
    }
    
    public static List<Map.Entry<String, Integer>> toList(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> result = new ArrayList<Map.Entry<String,Integer>>(map.entrySet());
        Collections.sort(result, new EntryComparator());
        return result;
    }
    
    public static class EntryComparator implements Comparator<Map.Entry<String, Integer>> {
        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            int valCmp = o1.getValue().compareTo(o2.getValue());
            
            if (valCmp == 0) {
                return o1.getKey().compareTo(o2.getKey());
            } else {
                return valCmp;
            }
        }
    }
    
    private CohNode proxy() {
        return cloud.node("proxy");
    }
    
    private CohNode all() {
        return cloud.all();
    }
}
