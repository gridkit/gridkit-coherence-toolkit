package org.gridkit.coherence.example;

import com.tangosol.net.cache.ReadWriteBackingMap;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ConcurrentMap;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.MapTrigger;

@SuppressWarnings("serial")
public class NewDecoratingTrigger implements MapTrigger {

	private static final int DECO_NEW = ExternalizableHelper.DECO_APP_1;
	private static final Binary TRUE = ExternalizableHelper.toBinary(Boolean.TRUE);
	
	@Override
	public void process(Entry e) {
		BinaryEntry be = (BinaryEntry) e;
		if (e.isPresent()) {
			ReadWriteBackingMap rwbm = (ReadWriteBackingMap) be.getBackingMap();
			ConcurrentMap cmap = rwbm.getControlMap();			
			cmap.lock(be.getBinaryKey(), -1);
			try {
				Binary val = (Binary) rwbm.getInternalCache().get(be.getBinaryKey());
				if (val == null || (isNew(val) && isStorePending(val))) {
					System.out.println("Decorate NEW " + e.getKey() + " -> " + e.getValue() + (isNew(be.getBinaryValue()) ? " (already set)" : ""));
					be.updateBinaryValue(ExternalizableHelper.decorate(be.getBinaryValue(), DECO_NEW, TRUE));
				}
				else if (isNew(be.getBinaryValue())) {
					System.out.println("Undecorate NEW " + e.getKey() + " -> " + e.getValue());
					be.updateBinaryValue(ExternalizableHelper.undecorate(be.getBinaryValue(), DECO_NEW));
				}
				else {
					System.out.println("Skipping " + e.getKey() + " -> " + e.getValue());
				}
			}
			finally {
				cmap.unlock(be.getBinaryKey());
			}
		}
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
