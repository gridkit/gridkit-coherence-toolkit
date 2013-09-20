package org.gridkit.coherence.example;

import java.util.HashSet;
import java.util.Set;

import com.tangosol.net.cache.BinaryEntryStore;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EraseOptimizerStoreWrapper implements BinaryEntryStore {
	
	private static final int DECO_NEW = ExternalizableHelper.DECO_APP_1;

	private BinaryEntryStore nested;

	public EraseOptimizerStoreWrapper(BinaryEntryStore nested) {
		this.nested = nested;
	}

	protected boolean shouldIgnore(BinaryEntry e) {
		Binary val = e.getOriginalBinaryValue();
		if (isNew(val) && isStorePending(val)) {
			System.out.println("IGNORE ERASE " + e.getKey() + " -> " + e.getOriginalValue() + (isNew(val) ? " DECO_NEW" : "") + (isStorePending(val) ? " STORE_PENDING" : ""));
			return true;
		}
		else {
			System.out.println("PROCESS ERASE " + e.getKey() + " -> " + e.getOriginalValue() + (isNew(val) ? " DECO_NEW" : "") + (isStorePending(val) ? " STORE_PENDING" : ""));
			return false;
		}
	}
	
	public void erase(BinaryEntry entry) {
		if (!shouldIgnore(entry)) {
			nested.erase(entry);
		}
	}

	public void eraseAll(Set entries) {
		entries = new HashSet(entries);
		for(Object e: entries) {
			if (shouldIgnore((BinaryEntry) e)) {
				entries.remove(e);
			}
		}
		nested.eraseAll(entries);
	}

	public void load(BinaryEntry entry) {
		nested.load(entry);
	}

	public void loadAll(Set entries) {
		nested.loadAll(entries);
	}

	public void store(BinaryEntry entry) {
		nested.store(entry);
	}

	public void storeAll(Set entries) {
		nested.storeAll(entries);
	}
	
	private boolean isStorePending(Binary val) {
		try {
			return ExternalizableHelper.getDecoration(val, ExternalizableHelper.DECO_STORE) != null;
		}
		catch(RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private boolean isNew(Binary val) {
		try {
			return ExternalizableHelper.getDecoration(val, DECO_NEW) != null; 
		}
		catch(RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}
}
