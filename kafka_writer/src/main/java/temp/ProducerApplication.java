package temp;


import java.io.IOException;
 
public class ProducerApplication  {

		public static void main(final String[] commandLineArguments) throws IOException {
			
			KafkaWriter kafka = new KafkaWriter();
			
			if (kafka.kafkaIsReachable()) {
				kafka.writeToKafka("mytopic", "testapp", "this is line 1");
				kafka.writeToKafka("mytopic", "testapp", "this is line 2");
				kafka.writeToKafka("mytopic", "testapp", "this is line 3");
				kafka.writeToKafka("mytopic", "testapp", "this is line 4");
			} else {
				System.err.println("ALERT: Cannot reach Kafka....");
			}
		}
}