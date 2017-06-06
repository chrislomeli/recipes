package com.data.validation.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import com.splunk.HttpService;
import com.splunk.JobExportArgs;
import com.splunk.ResultsReaderXml;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;

public class SplunkFilesyncService   {

	private static final Log log = LogFactory.getLog(SplunkFilesyncService.class);
	private Service splunkService=null;
	private String splunkHostName = "[SPLUNK-APPLIACTION-URL]";		
	private int splunkAPIPort =  8089;		
	private String splunkUser = "[SPLUNK-USER]";		
    private String splunkPassword = "[SPLUNK-PASSWORD]";		
	
	static String searchString =
			"search index=* sourcetype=myTopic | eval day=strftime(_time, \"%Y-%m-%d\") |  stats count by day sourcetype sourceorg";

	/**
	 * 
	 * Basic splunk connection
	 */
	public void basicConnect() {
		int retry  = 0;
		
		if (splunkService!=null)
			splunkService.logout();
		
		while (retry <= 5) {
			
			try {
				HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);	 
				int splunkPort =splunkAPIPort;
				splunkService = new Service(splunkHostName, splunkPort);
				String credentials = splunkUser+":"+splunkPassword;
				String basicAuthHeader = com.sun.org.apache.xerces.internal.impl.dv.util.Base64.encode(credentials.getBytes());
				splunkService.setToken("Basic " + basicAuthHeader);
				return;
				
			} catch (Exception e) {
				if ( retry++ >= 5 ) {
					e.printStackTrace();
					throw e;
				}
				log.info(String.format("Connect failed : retry %d", retry));
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						
					}
			}
		}
	}
	
	/**
	 * Normally we are calling this one timeslice at a time
	 * 
	 * @param searchString
	 * @param earliestTimestamp
	 * @param latestTimestamp
	 * @return
	 */
	public int performExportSearch( DateTime earliestTimestamp, DateTime latestTimestamp )  {
		
		String earliestTime = earliestTimestamp.toString();
		String latestTime = latestTimestamp.toString();
		log.info(String.format("%s: Do Start %s --> End %s",  earliestTime, latestTimestamp));

		/*
		 * Connect to Splunk
		 */
		basicConnect();

		/* set the splunk search, including earliest and latest times for this iteration*/
		JobExportArgs exportArgs = new JobExportArgs();
		exportArgs.setEarliestTime(earliestTime);
		exportArgs.setLatestTime(latestTime);
		exportArgs.setOutputMode(JobExportArgs.OutputMode.XML);
        InputStream exportStream = splunkService.export(searchString, exportArgs);
		ResultsReaderXml resultsReader;

       List<HashMap<String,String>>  records = new LinkedList<>();
		int processed_total=0;

		try {
			
			/* perform the actual search */
			resultsReader = new ResultsReaderXml(exportStream);
			log.info("\tSearch completed...");
	
			/* process outputs onto a list of events - in normal processing we are writing to impala or postgres evey 500 records*/
			 HashMap<String, String> event;
			 while ((event = resultsReader.getNextEvent()) != null) { 
				 processed_total++;
	   	    	  	records.add(event);
	   	    	  	if (records.size() >= 500) {
   	 				    log.info(String.format("\t\tStatus: processed %d files so far... ", processed_total));
	   					// normally we write to database here
	   					records.clear();
	   	    	  	}
    	    }
			
    	  	if (records.size() > 0) {
				log.info(String.format("\t\tDone: processed %d files ... ", processed_total));
				// normally we write to database here
				records.clear();
				records=null;
    	  	}
		
			 //
			// next time loop
			resultsReader.close();
			splunkService.logout();
			splunkService = null;

		} catch (IOException e) {
			log.error("FAILED SEARCH ");
			return -1;
		}
			
		return processed_total;
	}
	
}  
	

	
