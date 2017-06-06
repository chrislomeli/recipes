# Splunk reader
An example of exporting data from Splunk.


## motivation
The Splunk SDK provides a few different ways to read data out of Splunk.  The best for applications is the export mode, and while the documentation is good, it really helps to see a working version.  

For this example, we'll pull a summary of all of the files Splunk has ingested for a given topic (SourceType).  Splunk has a very good ingestion architecture for raw text files.  In some cases you'll want the same data in Splunk for exploratory data analysis, and also in Hive, Impala, or HBase for bigger analytical tasks.  Unless you are using the Splunk forwarder to feed both streams (it's a little problematic) you'll have two independednt ingestion streams.  In that case is really nice to be able to use the Splunk file information to validate your own ingestion and recover both ways if required.
 
* SplunkFilesyncApplication: main class
* SplunkFilesyncService:  the real worker



