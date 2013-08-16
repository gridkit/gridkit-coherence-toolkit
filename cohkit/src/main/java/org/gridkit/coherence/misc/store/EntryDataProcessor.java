/**
 * Copyright 2013 Alexey Ragozin
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

import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Analog for {@link EntryProcessor} with also receives custom data from
 * client.
 * 
 * This interface works with {@link DataSplittingProcessor} to effectively delivery data
 * along side with processing code.
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public interface EntryDataProcessor {

	public Object process(EntryWithData entry);
	
	public static interface EntryWithData {
		
		public BinaryEntry getEntry();
		
		public Object getPayloadData();
		
	}	
}
