/**
 * Copyright 2011-2013 Alexey Ragozin
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
package org.gridkit.coherence.misc.extend;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.ConfigurableCacheFactory;
import com.tangosol.net.DefaultConfigurableCacheFactory;
import com.tangosol.net.InvocationService;
import com.tangosol.net.NamedCache;
import com.tangosol.net.Service;
import com.tangosol.run.xml.XmlElement;
import com.tangosol.run.xml.XmlHelper;

/**
 * This class helps to manage private {@link ConfigurableCacheFactory} and dedicated Coherence*Extend connections.
 * Helpful is you need create multiple Extend connections to same cache/service in same or different clusters. 
 * <br/><br/>
 * <b>Copy/past friendly</b> - no dependencies outside of java file. 
 * 
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class ExtendConnection {

	private static Logger LOGGER = Logger.getLogger(ExtendConnection.class.getName());

	private static AtomicInteger CONNECTION_COUNTER = new AtomicInteger();

	private int connectionId = CONNECTION_COUNTER.incrementAndGet();
	private ConfigurableCacheFactory cacheFactory;
	
	/**
	 * @param configFile - path to XML cache configuration file (e.g. cache-config.xml)
	 */
	public ExtendConnection(String configFile) {
		cacheFactory = initPrivateCacheFactory(configFile);
	}

	private DefaultConfigurableCacheFactory initPrivateCacheFactory(String configFile) {
		LOGGER.info("New Extend connection #" + connectionId + " is going to be created, config: " + configFile);

		XmlElement xml = XmlHelper.loadFileOrResource(configFile, "Coherence cache configuration for Extend connection #" + connectionId);
		// transforming configuration
		XmlElement schemes = xml.getSafeElement("caching-schemes");
		for(Object o: schemes.getElementList()) {
			XmlElement scheme = (XmlElement) o;
			if (isRemoteScheme(scheme)) {
				String name = scheme.getSafeElement("service-name").getString();
				if (name != null) {
					String nname = name + "-" + connectionId;
					scheme.getElement("service-name").setString(nname);
				}
			}
		}
		
		DefaultConfigurableCacheFactory factory = new DefaultConfigurableCacheFactory(xml);
		return factory;
	}
	
    
    private boolean isRemoteScheme(XmlElement scheme) {
		String name = scheme.getName();
		return "remote-cache-scheme".equals(name) || "remote-invocation-scheme".equals(name);
	}

    /** 
     * Same as {@link CacheFactory#getCache(String)} but using private cache factory and scoped remote service (if cache is mapped to remote service). 
     * @param name - cache name
     */
    public NamedCache getCache(String name) {
        return cacheFactory.ensureCache(name, null);
    }

    /** 
     * Same as {@link CacheFactory#getService(String)} but service name will scoped with id of this connection instance. 
     */
	public InvocationService ensureInvocationService(String serviceName) {
		return (InvocationService) cacheFactory.ensureService(serviceName + "-" + connectionId);
	}

    /**
     * Shuts down all services belonging to this connection, effectively releasing resources.
     * It is safe to throw object away to GC after shutdown. 
     */
	public void disconnect() {
		XmlElement xml = cacheFactory.getConfig();
		// finding service names
		XmlElement schemes = xml.getSafeElement("caching-schemes");
		for(Object o: schemes.getElementList()) {
			XmlElement scheme = (XmlElement) o;
			if (isRemoteScheme(scheme)) {
				String name = scheme.getSafeElement("service-name").getString();
				if (name != null) {
					// Note: static CacheFactory is used here. It is possible beacuse service names has global scope.
					try {
						Service service = CacheFactory.getService(name);
						if (service.isRunning()) {
							service.shutdown();
							LOGGER.info("Service stopped: " + service);
						}
					}
					catch(Exception e) {
						LOGGER.log(Level.WARNING, "Exception during service shutdown", e);
					}
				}
			}
		}
	}
	
	public String toString() {
		return "ExtendConnection-" + connectionId;
	}
}
