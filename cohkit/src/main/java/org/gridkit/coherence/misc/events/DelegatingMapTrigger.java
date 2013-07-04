/**
 * Copyright 2012-2013 Alexey Ragozin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gridkit.coherence.misc.events;

import com.oracle.coherence.common.events.dispatching.listeners.DelegatingBackingMapListener;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.MapListener;
import com.tangosol.util.MapTrigger;

/**
 * This class is implementing {@link DelegatingBackingMapListener} functionality,
 * but is called as {@link MapTrigger}.
 * <br/>
 * <a href="http://blog.ragozin.info/2012/12/coherence-101-beware-of-cache-listeners.html">This article</a>
 * explains motivation behind this class. 
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public abstract class DelegatingMapTrigger extends TriggerToBMListenerAdapter {

	private static final long serialVersionUID = 20121214L;

	@Override
	protected MapListener instantiateListener(String cacheName, BackingMapManagerContext context) {
		return new DelegatingBackingMapListener(context, cacheName);
	}	
}
