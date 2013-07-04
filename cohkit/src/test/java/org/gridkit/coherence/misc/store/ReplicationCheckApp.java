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
package org.gridkit.coherence.misc.store;


public class ReplicationCheckApp {
//	public static void main(String[] args) {
//	    ViCluster cluster = new ViCluster("replicationCheck", "com.tangosol", "org.gridkit");
//        CohHelper.enableFastLocalCluster(cluster);
//        CohHelper.enableJmx(cluster);
//        
//        cluster.setProp("org.gridkit.batch-store-uploader.partition-count", "1");
//        cluster.setProp("tangosol.coherence.cacheconfig", "batch-uploader-test-cache-config.xml");
//        
//        CohHelper.localstorage(cluster.node("client"), false);
//        
//        String cacheService = cluster.node("client").getServiceNameForCache("store-cache"); 
//        
//        cluster.node("storage1").getService(cacheService);
//        
//        cluster.node("client").exec(new Runnable() {
//            @Override
//            public void run() {
//                NamedCache cache = CacheFactory.getCache("store-cache");
//                BatchStoreUploader uploader = new BatchStoreUploader(cache);
//                uploader.put("1", "test");
//                uploader.put("2", "test2");
//                uploader.flush();
//            }
//        });
//
//        
//        cluster.node("storage2").getService(cacheService);
//        
//        CohHelper.jmxWaitForStatusHA(cluster.node("storage2"), cacheService, "NODE-SAFE");
//
//        cluster.node("storage1").kill();
//        
//        NamedCache cache = cluster.node("client").getCache("store-cache");
//        System.out.println(cache.get("1"));
//        System.out.println(cache.get("2"));
//        
//        cluster.shutdown();
//	}
}
