package org.gridkit.coherence.misc.query;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.util.Filter;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * <p>
 * This is a very special filter which can help you to test whenever matching equals filter would return empty data set or not.
 * </p>
 * <p>
 * <b>Rationale</b><br/>
 * Sometimes you need to know if there are any entry matching criteria or not (without knowing exact entries or their number).
 * <br/>
 * You can use {@link Count} aggregator, but if matching result set is large it will require some work effort and produce considerable stress on garbage collector.
 * <br/>
 * If you want to perform such test, say, every 50ms that may not be acceptable.
 * <br/>
 * </p>
 * <p>         
 * This filter is doing simple check in index, which is required, and return first entry reference found in index (avoiding creation of temporary entry set object, which could be fairly large).
 * </p>
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class IndexedPresenceFilter implements IndexAwareFilter, Serializable, PortableObject {
	
	private static final long serialVersionUID = 20130731L;

	private ValueExtractor extractor;
	private Collection<?> values;

	/**
	 * @deprecated Required for POF serialization
	 */
	public IndexedPresenceFilter() {
	}
	
	public IndexedPresenceFilter(ValueExtractor extractor, Object value) {
		this.extractor = extractor;
		this.values = Collections.singleton(value);
	}

	public IndexedPresenceFilter(ValueExtractor extractor, Collection<?> values) {
		this.extractor = extractor;
		this.values = values;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean evaluateEntry(Entry entry) {
		throw new UnsupportedOperationException("This is index-only filter");
	}

	@Override
	public boolean evaluate(Object value) {
		throw new UnsupportedOperationException("This is index-only filter");
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Filter applyIndex(Map indexMap, Set candidates) {
		MapIndex index = (MapIndex) indexMap.get(extractor);
		if (index == null) {
			throw new UnsupportedOperationException("Index by " + extractor + " is required for this filter");
		}
		for(Object val: values) {
			Collection<?> match = (Collection<?>) index.getIndexContents().get(val);
			if (match != null && !match.isEmpty()) {
				for(Object k: match) {
					if (candidates.contains(k)) {
						candidates.retainAll(Collections.singleton(k));
						return null;
					}
				}
			}
		}
		candidates.clear();
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int calculateEffectiveness(Map indexMap, Set candidates) {
		return 0;
	}

	@Override
	public void readExternal(PofReader in) throws IOException {
		extractor = (ValueExtractor) in.readObject(1);
		values = (Collection<?>) in.readObject(2);
	}

	@Override
	public void writeExternal(PofWriter out) throws IOException {
		out.writeObject(1, extractor);
		out.writeObject(2, values);
	}
}
