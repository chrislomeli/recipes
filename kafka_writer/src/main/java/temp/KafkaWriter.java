package temp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * 
 * @author clomeli
 *
 */
public class KafkaWriter  {

	static int CONSECUTIVE_FAILURES = 0;
	static int WORKING_STATUS = 0;  //0=nothing, 1=ok, -1 errors
	public static int RECORDCOUNT=0;
	
	String KAFKA_LISTENERS ="localhost:9092";
	String KAFKA_ACKS = "1";
	String KAFKA_TIMEOUT = "1000";
	int PING_TIMEOUT = 5*1000;  // 5 seconds

//	String KAFKA_LISTENERS ="192.167.101.8:9092,192.166.190.235:9092,192.34.50.248:9092";  // looks more like this in production
//	String KAFKA_ACKS = 2;
//	String KAFKA_TIMEOUT = 3000;
	
	
	private final static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYYMMddHH");
	
    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
	
	// Avro
	KafkaProducer<String, byte[]> byteproducer = null;

	/*
	 * constructor - create Kafka 
	 */
	public KafkaWriter() {
		try {
			// Kafka
			String kafkaConnection = KAFKA_LISTENERS;
			Properties props = new Properties();
            props.put("bootstrap.servers", kafkaConnection);
            props.put("acks", KAFKA_ACKS);  // only really need 2
            props.put("request.timeout.ms", KAFKA_TIMEOUT);  // fail quickly for testing
            props.put("retries", 0);
            props.put("batch.size", "16384");
            props.put("buffer.memory", "33554432");
            props.put("linger.ms", "0");
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());
        	
 	        byteproducer = new KafkaProducer<>(props);
	        
		} catch (Exception e) {
			e.printStackTrace();
		}
 	}

	
	public ProducerDataWrapper createMyProducerBean(String s, String t, String d){
		return new ProducerDataWrapper(t,s,d);
	}

	public int writeToKafka(String topic, String source, String inputString) {
		ProducerDataWrapper ar = createMyProducerBean(source, topic, inputString );
		return writeToKafka(ar.producerData);
	}
	
	public int writeToKafka(ProducerData ar) {
		byte[] bytes = makeAvroObject(ar, ProducerData.class);

		ProducerRecord<String, byte[]> record = new ProducerRecord<>(ar.topic, bytes);

		boolean sent = false;
	    try {
	        
			for (int i=0; i<3 && !sent; i++ ) {
				try {
					System.out.println("Batching one kafka producer record ...");
					byteproducer.send(record, new HandleResults(ar));
					sent = true;
					break;
				} catch (Exception e) {
					e.printStackTrace();
					try {Thread.sleep(6000); } catch (Exception ex){}
				}
			}
	        
	    } finally {

	    }
		if (!sent) {
			return 0;
		}
		
		RECORDCOUNT++;
		return 1;
	}
	
	
	/*
	 * AVRO
	 * 
	 */
	public byte[] makeAvroObject(Object value, Type classType) {
		try {
			 Schema s = ReflectData.get().getSchema(classType);
			  ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(s);
			  ByteArrayOutputStream out = new ByteArrayOutputStream();
			  writer.write(value, EncoderFactory.get().directBinaryEncoder(out, null));
			  byte[] bytes = out.toByteArray();
			  return bytes;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	
	public boolean kafkaIsReachable() {  /* not completely tested  */
		String[] listeners = KAFKA_LISTENERS.split(",");
		
		System.out.println(String.format("pinging kafka listeners..."));
		
		for (int i=0; i<listeners.length; i++) {
			String[] dataum = listeners[i].split(":");
			String server = dataum[0];
			int localPort = Integer.parseInt(dataum[1]);
			System.out.print(String.format("\ttesting %s : %d ...", server, localPort));
			Socket clientSocket=null;
		    try {
		    	InetAddress inteAddress = InetAddress.getByName(server);
		        clientSocket = new Socket();
		        clientSocket.connect(new InetSocketAddress(inteAddress, localPort), PING_TIMEOUT);
		        clientSocket.close();
		    	System.out.println(" = connected...");
		        return true;
		      }
		      catch (Exception e) {
		    	  System.out.println(e.getMessage());
		    	  return false;
		      }
		}
		
		return false;
	}
	
	
	
	/*
	 * 
	 */
	public class HandleResults implements Callback {
		ProducerData avro;
		
		public HandleResults(ProducerData av) {
			avro=av;
		}
		
		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {

			if (exception == null) {
				CONSECUTIVE_FAILURES = 0;
				WORKING_STATUS=1;
			}
			else {
				if (CONSECUTIVE_FAILURES>10) 
					WORKING_STATUS=-1;
				try {
					spoolToJSON(avro);
				} catch (IOException e) {
					WORKING_STATUS=-1;
					e.printStackTrace();
				}
			}
		}
	}

	
	public void spoolToJSON(ProducerData avro) throws IOException {
		String json  = JSON.writeValueAsString(avro);
		if (json==null) {
			return;
		}
		System.err.println("SPOOL for later: "+json);
		//JSON.writeValue(fileWriter, json.replaceAll("\n"," "));
//		FileWriter fileWriter = new FileWriter(getSpoolFile(spoolDir, avro.topic), true);
//		fileWriter.write(json.replaceAll("\n"," "));
//		fileWriter.write("\n");
//		fileWriter.close();
	}

	
	public void close() {
		this.byteproducer.close();
		
	}

	

}
