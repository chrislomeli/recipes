## filesystem crawler
An example of crawling through a filesystem and maintaining the files in a data store - in this case jdbc.


## motivation
In most case we can and should use an existing agent - like a 'beat', logstash, or flume, or even the aws java file tailer
Sometimes we need a bit more control and the pain of asking a scala or java developer to learn and torture those tools into submission is more than it's worth.   This is the directory walker part of such a custom solution.  

This recipe is primarily for tailing text files that we can't get any other way and need to ship line-by-line to kafka or kinesis.  In this case we need a persistence module to record the file pointers of our last read from the file.  The first inclination is to use a tiny local sql engine like H2 or even a text file to store the file pointers, but in some cases there's a benefit in storing file names and pointers in a central place.  You can then control the tailer and it's actions in one place rather than logging into each agent's server.

## contents
* FileCrawler: main class
* FileVistor:  nio file walker worker - this is where the logic resides
* SimpleDAO: seriously simple connection and data access object



