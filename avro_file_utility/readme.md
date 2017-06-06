# avro file utility
A simple utility to demonstrate reading and storing a non-serialized avro file to local disk.

## motivation
During ingestion and testing it's sometimes handy to spool a file off to disk and pull it back in later.
Normally I would either serialize the bean or dump to json, but if you already have an avro object it's just as simple for short term
This is a quick working example for anyone who who needs it. 

* ProducerData is a simple bean without any embellishment
* ProducerDataWrapper manages the bean
* AvroFileUtility
** main function just creates a few beans in code
** then call writeToAvro to write to a local file
** then readFromAvro to read the file back


