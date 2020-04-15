# -bigdata-kafka-java-apps

## Follow the steps below for running the Custom Consumer and Producer

### Start Zookeeper

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties


### Start kafka server

.\bin\windows\kafka-server-start.bat .\config\server.properties

### Start the Consumer 

Here the topic name is test and group name is group1

java -cp target/kafkapart2-1.0-SNAPSHOT-jar-with-dependencies.jar com.mycompany.kafkapart2.Consumer test group1

### Start the Producer

java -cp target/kafkapart2-1.0-SNAPSHOT-jar-with-dependencies.jar com.mycompany.kafkapart2.Producer test

In the producer Terminal you will be prompted to enter the text that want to be encrypted. After you are done with entering the text you can find the encrypted text on the consumer terminal

