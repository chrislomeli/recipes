# kafka writer
A simple utility to demonstrate publishing an avro file (via reflection) to a kafka topic.

## motivation
Writing to kafka is pretty straightforward, but we get developers who have worked on other brokers and it helps to have a simple working program to step through that does not take a lot of setup.

* ProducerData: is a simple bean without any embellishment
* ProducerDataWrapper:  manages the ProducerData bean
* Kafka writer: 
** translate the ProducerData to an avro object -- this is not really required - you could just convert json or any other form to a byte array 
** send to kafka 


