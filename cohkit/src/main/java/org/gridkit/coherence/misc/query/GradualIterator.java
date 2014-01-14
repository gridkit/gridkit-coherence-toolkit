package org.gridkit.coherence.misc.query;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.PartitionedFilter;

@SuppressWarnings({"rawtypes", "unchecked", "serial"})
public class GradualIterator {
    private NamedCache cache;
    private int partitionCount;
    
    private Filter filter = AlwaysFilter.INSTANCE;
    private ValueExtractor extractor = IdentityExtractor.INSTANCE;
    private Comparator keyComparator = new NaturalComparator();
    private Comparator valueComparator = new NaturalComparator();
    private int limit = 32;
    private boolean clearValue = true;
    
    private Entry offset;
    private PartitionSet partitionSet;
    
    public GradualIterator(NamedCache cache) {
        this(cache, ((PartitionedService)cache.getCacheService()).getPartitionCount());
    }

    public GradualIterator(NamedCache cache, int partitionCount) {
        this.cache = cache;
        this.partitionCount = partitionCount;
        this.offset = null;
        this.partitionSet = new PartitionSet(partitionCount);
        this.partitionSet.invert();
    }
    
    public boolean hasNext() {
        return !partitionSet.isEmpty();
    }

    public List<Entry> getNext() {
        if (!hasNext()) {
            throw new IllegalStateException();
        }
        
        MasterAggregator aggregator = newMasterAggregator(limit);

        Result result = (Result)cache.aggregate(new PartitionedFilter(filter, partitionSet), aggregator);
        
        if (result.values.size() > 0) {
            this.offset = new Entry(result.values.get(result.values.size()-1));
        }
        this.partitionSet = result.tailPartitions;
        
        if (clearValue) { // TODO clear tail array
            for (Entry entry : result.values) {
                entry.value = null;
            }
        }
        
        return result.values;
    }

    private MasterAggregator newMasterAggregator(int limit) {
        MasterAggregator result = new MasterAggregator();
        result.limit = limit;
        result.offset = this.offset;
        result.extractor = this.extractor;
        result.keyComparator = this.keyComparator;
        result.valueComparator = this.valueComparator;
        result.partitionCount = this.partitionCount;
        result.comparator = new ValueComparator(keyComparator, valueComparator);
        return result;
    }
    
    public static class MasterAggregator extends BaseAggregator implements ParallelAwareAggregator {
        private static final long serialVersionUID = -620150976044705822L;
        
        @Override
        public Object aggregate(Set rawEntries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object aggregateResults(Collection rawResults) {
            Collection<Result> results = (Collection<Result>)rawResults;
            
            List<Entry> values = values(results);
            PartitionSet tailPartitions = tailPartitions(results);
            
            Result result = new Result();
            
            if (tailPartitions.isEmpty()) {
                result.values = values;
                result.tailPartitions = tailPartitions;
            } else {
                Entry pivot = pivot(results);
                
                int index = Collections.binarySearch(values, pivot, comparator);
                
                List<Entry> head = head(values, index + 1);
                List<Entry> tail = tail(values, index + 1);
                
                result.values = head;
                result.tailPartitions = tailPartitions;
                result.tailPartitions.add(tailPartitions(tail));
            }

            return result;
        }

        private Entry pivot(Collection<Result> results) {
            List<Entry> pivots = new ArrayList<Entry>(results.size());

            for (Result result : results) {
                if (!result.values.isEmpty()) {
                    pivots.add(result.values.get(result.values.size() - 1));
                }
            }
            
            return Collections.min(pivots, comparator);
        }
        
        private List<Entry> head(List<Entry> values, int index) {
            return values.subList(0, Math.min(index, values.size()));
        }
        
        private List<Entry> tail(List<Entry> values, int index) {
            if (values.size() > index) {
                return values.subList(index, values.size());
            } else {
                return Collections.emptyList();
            }
        }
        
        private List<Entry> values(Collection<Result> results) {
            int size = 0;
            for (Result result : results) {
                size += result.values.size();
            }
            
            List<Entry> values = new ArrayList<Entry>(size);
            for (Result result : results) {
                values.addAll(result.values);
            }
            
            Collections.sort(values, comparator);

            return values;
        }
        
        private PartitionSet tailPartitions(Collection<Result> results) {
            PartitionSet result = new PartitionSet(partitionCount);
            for (Result sResult : results) {
                result.add(sResult.tailPartitions);
            }
            return result;
        }
        
        private PartitionSet tailPartitions(List<Entry> values) {
            PartitionSet result = new PartitionSet(partitionCount);
            for (Entry value : values) {
                result.add(value.partition);
            }
            return result;
        }
        
        @Override
        public EntryAggregator getParallelAggregator() {
            SlaveAggregator result = new SlaveAggregator();
            result.limit = this.limit;
            result.offset = this.offset;
            result.extractor = this.extractor;
            result.keyComparator = this.keyComparator;
            result.valueComparator = this.valueComparator;
            result.partitionCount = this.partitionCount;
            result.comparator = new ValueComparator(keyComparator, valueComparator);
            return result;
        }
    }
    
    public static class SlaveAggregator extends BaseAggregator implements EntryAggregator {
        private static final long serialVersionUID = 5955343768305648954L;

        @Override
        public Object aggregate(Set rawEntries) {
            Set<BinaryEntry> entries = (Set<BinaryEntry>)rawEntries;
            
            List<Entry> values = new ArrayList<Entry>();
            
            BackingMapManagerContext managerContext = null;
            
            PartitionSet partitions = new PartitionSet(partitionCount);
            
            for (BinaryEntry binEntry : entries) {
                Entry entry = new Entry();
                
                if (managerContext == null) {
                    managerContext = binEntry.getContext();
                }
                
                entry.partition = managerContext.getKeyPartition(binEntry.getBinaryKey());
                entry.value = binEntry.extract(extractor);
                entry.key = binEntry.getKey();
                
                partitions.add(entry.partition);
                
                if (comparator.compare(offset, entry) < 0) {
                    values.add(entry);
                }
            }
            
            return truncate(values, partitions.cardinality() * limit);
        }
        
        private Result truncate(List<Entry> values, int size) {
            Collections.sort(values, comparator);
            
            Result result = new Result();
            
            int resultSize = Math.min(values.size(), size);
            result.values = new ArrayList<Entry>(resultSize);
            result.tailPartitions = new PartitionSet(partitionCount);
            
            for (int i = 0; i < resultSize; ++i) {
                result.values.add(values.get(i));
            }
            
            for (int i = resultSize; i < values.size(); ++i) {
                result.tailPartitions.add(values.get(i).partition);
            }
            
            return result;
        }
    }
    
    private static class ValueComparator implements Comparator<Entry>, Serializable {
        private final Comparator keyComparator;
        private final Comparator valueComparator;
        
        public ValueComparator(Comparator keyComparator, Comparator valueComparator) {
            this.keyComparator = keyComparator;
            this.valueComparator = valueComparator;
        }

        @Override
        public int compare(Entry o1, Entry o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            }
            
            int result = valueComparator.compare(o1.value, o2.value);
            
            if (result == 0) {
                return keyComparator.compare(o1.key, o2.key);
            } else {
                return result;
            }
        }
    }
    
    public static class Result implements PortableObject, Serializable {
        public List<Entry> values;
        public PartitionSet tailPartitions;
        
        @Override
        public void readExternal(PofReader reader) throws IOException {
            int id = 0;
            values = (List<Entry>)reader.readCollection(id++, new ArrayList<Entry>());
            tailPartitions = (PartitionSet) reader.readObject(id++);
        }
        
        @Override
        public void writeExternal(PofWriter writer) throws IOException {
            int id = 0;
            writer.writeCollection(id++, values, Entry.class);
            writer.writeObject(id++, tailPartitions);
        }
    }
    
    public static class Entry implements PortableObject, Serializable {
        public Object key;
        public Object value;
        public int partition;

        public Entry() {} // for POF
        
        public Entry(Entry entry) {
            this.key = entry.key;
            this.value = entry.value;
            this.partition = entry.partition;
        }

        public <T> T getKey() {
            return (T) key;
        }
        
        public <T> T getValue() {
            return (T) value;
        }
        
        @Override
        public void readExternal(PofReader reader) throws IOException {
            int id = 0;
            partition = reader.readInt(id++);
            value = reader.readObject(id++);
            key = reader.readObject(id++);
        }
        
        @Override
        public void writeExternal(PofWriter writer) throws IOException {
            int id = 0;
            writer.writeInt(id++, partition);
            writer.writeObject(id++, value);
            writer.writeObject(id++, key);
        }
    }

    public static class BaseAggregator implements PortableObject, Serializable {
        public Entry offset;
        public int limit;
        public ValueExtractor extractor;
        public Comparator keyComparator;
        public Comparator valueComparator;
        public int partitionCount;
        
        public Comparator<Entry> comparator;
        
        @Override
        public void readExternal(PofReader reader) throws IOException {
            int id = 0;
            offset = (Entry) reader.readObject(id++);
            limit = reader.readInt(id++);
            extractor = (ValueExtractor) reader.readObject(id++);
            keyComparator = (Comparator) reader.readObject(id++);
            valueComparator = (Comparator) reader.readObject(id++);
            partitionCount = reader.readInt(id++);
            comparator = new ValueComparator(keyComparator, valueComparator);
        }
        
        @Override
        public void writeExternal(PofWriter writer) throws IOException {
            int id = 0;
            writer.writeObject(id++, offset);
            writer.writeInt(id++, limit);
            writer.writeObject(id++, extractor);
            writer.writeObject(id++, keyComparator);
            writer.writeObject(id++, valueComparator);
            writer.writeInt(id++, partitionCount);
        }
    }

    public void setValueComparator(Comparator comparator) {
        if (comparator == null) {
            throw new NullPointerException();
        }
        this.valueComparator = comparator;
    }
    
    public void setKeyComparator(Comparator comparator) {
        if (comparator == null) {
            throw new NullPointerException();
        }
        this.keyComparator = comparator;
    }
    
    public void setExtractor(ValueExtractor extractor) {
        if (extractor == null) {
            throw new NullPointerException();
        }
        this.extractor = extractor;
    }
    
    public void setFilter(Filter filter) {
        if (filter == null) {
            throw new NullPointerException();
        }
        this.filter = filter;
    }
    
    public void setLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException();
        }
        this.limit = limit;
    }
    
    public void setClearValue(boolean clearValue) {
        this.clearValue = clearValue;
    }

    public static class NaturalComparator<T extends Comparable<T>> implements Comparator<T>, PortableObject, Serializable {
        public static enum NullMode {
            FIRST {
                @Override
                protected int compare(Object o1, Object o2) {
                    if (o1 == null) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            },
            LAST {
                @Override
                protected int compare(Object o1, Object o2) {
                    if (o1 == null) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            };
            
            protected abstract int compare(Object o1, Object o2);
        };
        
        private NullMode nullMode;
        
        public NaturalComparator() {
            this(NullMode.FIRST);
        }

        public NaturalComparator(NullMode nullMode) {
            if (nullMode == null) {
                throw new IllegalArgumentException();
            }
            this.nullMode = nullMode;
        }

        @Override
        public int compare(T o1, T o2) {
            if (o1 == o2) {
                return 0;
            } else if (o1 == null || o2 == null) {
                return nullMode.compare(o1, o2);
            } else {
                return o1.compareTo(o2);
            }
        }

        @Override
        public void readExternal(PofReader reader) throws IOException {
            nullMode = NullMode.values()[reader.readInt(0)];
        }

        @Override
        public void writeExternal(PofWriter writer) throws IOException {
            writer.writeInt(0, nullMode.ordinal());
        }
    }
}
