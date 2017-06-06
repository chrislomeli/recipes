package com.data.validation.service;

import org.joda.time.DateTime;


public class SplunkFilesyncApplication  {

	/**
	 * 
	 * Main executable method 
	 * 
	 * In some cases we need a high fidelity data set in impala or some other database that is also in a search solution
	 * like splunk.  Why would we want to do this?
	 * 
	 * Because solutions like Splunk, Solr, or elastic search provide a great view of the full file that is good for exploration and has not been limited by whatever schema we decided to place on the data in impala
	 * Usually we want a subset of the important join-able data in a schema based system, but we don't want to also lose the ability to search the entire file by line
	 * I can't speak for elasticsearch beats which also look great, or fluentd, or logstash, but Splunk has a very good log ingestion forwarder
	 * so we actually use Splunk filename and line count to validate our own staged records in impala
	 * this is a kernel of that solution.
	 * Normally we cycle through one time window at a time to limit the load on Splunk, which does handle big output that our Hadoop systems possess 
	 */
		public static void main(final String[] commandLineArguments)  {
			try {
				
				DateTime latestTimestamp = new DateTime();
				DateTime earliestTimestamp = latestTimestamp.minusHours(1);
				new SplunkFilesyncService().performExportSearch(earliestTimestamp, latestTimestamp);
				
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
}