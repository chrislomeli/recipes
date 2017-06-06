package temp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;


public class AvroFileUtility {

	static String SPOOL_PATH = "/var/local/foo/spool";
	

	/*
	 * Archive java data to a file
	 */
	public static void writeToAvroFile(ProducerData avro, String filename)  {

		// get the file writer for the Producer
		Schema schema = ReflectData.get().getSchema(ProducerData.class);
		ReflectDatumWriter<Object> datumWriter = new ReflectDatumWriter<Object>(schema);
		DataFileWriter<Object> dataFileWriter = new DataFileWriter<Object>(datumWriter);
		
		try {
			File f = new File(filename);
			if (!f.exists())
				dataFileWriter.create(schema,f);
			else 
				dataFileWriter.appendTo(f);
			
			dataFileWriter.append(avro);
			dataFileWriter.close();	
			System.out.println("appened data to file "+filename);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public static void readfromAvro(String filename)  {
		File file = new File( filename );
		
		Schema schema = ReflectData.get().getSchema(ProducerData.class);
		ReflectDatumReader<Object> datumReader = new ReflectDatumReader<Object>(schema);

		ProducerData avro = null;
		try (DataFileReader<Object> dataFileReader = new DataFileReader<Object>(file, datumReader)) {
			
			while (dataFileReader.hasNext()) {

				avro = (ProducerData) dataFileReader.next(avro);
				System.out.println(	String.format(
						"{timestamp: %s, topic: %s, source: %s, data: %s, hashkey: %s}", avro.timestamp, avro.topic, avro.source, avro.data, avro.hashkey));
				
			}
			dataFileReader.close();
			file.delete();
			
		} catch (Exception e) {
			
		} finally {
			
		}
	}

	/*
	 * Create a file containing avro records from an list of well-defined java objects 
	 * 	- stripped out any reflection or serialization
	 * Read the list back from the file
	 * 
	 */
	public static void main(final String[] commandLineArguments)  {

		String filename = "c:\\temp\\mytempfile.avro";
		
		// create some beans to save into avro
		List<ProducerDataWrapper> producerdata = new ArrayList<>();
		producerdata.add(new ProducerDataWrapper("mytopic", "orginal-file-name", "this is my first line"));
		producerdata.add(new ProducerDataWrapper("mytopic", "orginal-file-name", "this is my second line"));

		// write to avro file
		for (ProducerDataWrapper data : producerdata) 
			writeToAvroFile(data.producerData,filename);

		readfromAvro(filename);
		
		
	}
		
}
