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
import com.tangosol.util.filter.IndexAwareFilter;

public class IndexPresenceFilter implements IndexAwareFilter, Serializable, PortableObject {
	
	private static final long serialVersionUID = 20130731L;

	private ValueExtractor extractor;
	private Collection<?> values;

	/**
	 * @deprecated Required for POF serialization
	 */
	public IndexPresenceFilter() {
	}
	
	public IndexPresenceFilter(ValueExtractor extractor, Object value) {
		this.extractor = extractor;
		this.values = Collections.singleton(value);
	}

	public IndexPresenceFilter(ValueExtractor extractor, Collection<?> values) {
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
