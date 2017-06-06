package temp;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ProducerDataWrapper {

	ProducerData producerData;

	public ProducerDataWrapper(String t,  String s, String d) {
		producerData = new ProducerData();
		producerData.source = s;
		producerData.data = d;
		producerData.topic = t;
		producerData.timestamp=System.currentTimeMillis();
		producerData.hashkey = createHashkey(d);
	}
	
	 protected String createHashkey(String inputString)
	  {
	    String key = inputString;
	    try {
	      MessageDigest digest = MessageDigest.getInstance("SHA1");
	      digest.update(key.getBytes(), 0, key.length());
	      key = new BigInteger(1, digest.digest()).toString(16);
	    }
	    catch (NoSuchAlgorithmException localNoSuchAlgorithmException) {}
	    
	    return key;
	  }
	 
	 
	
}


