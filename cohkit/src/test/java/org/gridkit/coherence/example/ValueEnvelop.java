package org.gridkit.coherence.example;

import java.io.IOException;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofSerializer;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.util.Binary;

/**
 * Example of partial deserialization of object.<br/>
 * 
 * IMPORTANT: This is an example, to code meant to do something meaningful.
 *  
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class ValueEnvelop {

	public static final int PAYLOAD_POF 	=  10;
	
    protected Object payload;
    protected Binary binaryPayload;
    transient boolean serverMode;
    

    /** TO BE USED WITH SERIALIZER */
    protected ValueEnvelop(Object payload, Binary binaryPayload, boolean serverMode) {
		this.payload = payload;
		this.binaryPayload = binaryPayload;
		this.serverMode = serverMode;
	}

	public ValueEnvelop(Object payload, long timestamp) {
        this.payload = payload;
        this.serverMode = false;
    }
    
    public Object getPayload() {
        return payload;
    }
    
    public Binary getBinaryPayload() {
    	return binaryPayload;
    }
    	
    public static class ServerSerializer implements PofSerializer {

		@Override
		public Object deserialize(PofReader in) throws IOException {
			Binary data = in.readRemainder();			
			ValueEnvelop dv = new ValueEnvelop(null, data, true);
			return dv;
		}

		@Override
		public void serialize(PofWriter out, Object o) throws IOException {			
			ValueEnvelop dv = (ValueEnvelop) o;
			if (!dv.serverMode) {
				throw new IllegalArgumentException("Envelop is in client mode, but server serializer is used. Something wrong with DF POF config");
			}
			out.writeRemainder(dv.getBinaryPayload());
		}
    }
    
    public static class ClientSerializer implements PofSerializer {

		@Override
		public Object deserialize(PofReader in) throws IOException {
			Object payload = in.readObject(PAYLOAD_POF);
			Binary data = in.readRemainder();			
			ValueEnvelop dv = new ValueEnvelop(payload, data, false);
			return dv;
		}

		@Override
		public void serialize(PofWriter out, Object o) throws IOException {
			ValueEnvelop dv = (ValueEnvelop) o;
			if (dv.serverMode) {
				throw new IllegalArgumentException("Envelop is in server mode, but client serializer is used. Something wrong with DF POF config");
			}
			out.writeObject(PAYLOAD_POF, dv.getPayload());
			out.writeRemainder(dv.getBinaryPayload());
		}    	
    }    
}
