package org.gridkit.coherence.example;

import java.util.Comparator;
import java.util.List;

import org.gridkit.coherence.misc.query.GradualIterator;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.extractor.ReflectionExtractor;
import com.tangosol.util.filter.LessFilter;

public class GradualIteratorExample {
    
    /**
     * Simple example to describe GradualIterator API.
     * This code is only for demonstration and doesn't work.
     * For working example see GradualIteratorTest class.
     */
    public static void main(String args[]) {
        NamedCache cache = CacheFactory.getCache("cache");
        
        /**
         * Creating GradualIterator using instance of partitioned cache
         */
        GradualIterator iter = new GradualIterator(cache);
        
        /**
         * Setting filter to query
         */
        iter.setFilter(new LessFilter("number", 1024 * 1024));
        
        /**
         * Limiting number of cache keys and extracted order by values
         * transfered to this member in single iteration (GradualIterator.getNext() method).
         */
        iter.setLimit(1024);
        
        /**
         * Setting value extractor and comparator for sorting.
         * If extracted type implements Comparable then comparator isn't required.
         */
        iter.setExtractor(new ReflectionExtractor("string"));
        iter.setValueComparator(newValueComparator());
        
        /**
         * Setting cache key comparator if it doesn't implement Comparable
         */
        iter.setKeyComparator(newKeyComparator());
        
        /**
         * Iterating through cache entries matching query criteria
         */
        while (iter.hasNext()) {
            /**
             * Getting next portion of cache entries
             * Size is no more that limit specified above
             */
            List<GradualIterator.Entry> entries = iter.getNext();
            
            for (GradualIterator.Entry entry : entries) {
                /**
                 * Printing cache entry key and extracted order by value
                 */
                System.err.println("Cache key: " + entry.getKey());
                System.err.println("Extracted value: " + entry.getKey());
            }
        }
    }
    
    public static Comparator<?> newValueComparator() {
        return null;
    }
    
    public static Comparator<?> newKeyComparator() {
        return null;
    }
}
