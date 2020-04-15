# -bigdata-kafka-java-apps

## Steps that need to be followed  for running the Custom Consumer and Producer

### Start Zookeeper

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties


### Start kafka server

.\bin\windows\kafka-server-start.bat .\config\server.properties

### Start the Consumer 

Here the topic name is test and group name is group1

java -cp target/kafkapart2-1.0-SNAPSHOT-jar-with-dependencies.jar com.mycompany.kafkapart2.Consumer test group1

### Start the Producer

java -cp target/kafkapart2-1.0-SNAPSHOT-jar-with-dependencies.jar com.mycompany.kafkapart2.Producer test

You will be prompted  in the producer trerminal to enter the text that want to be encrypted. Once you are  done with  entering the text you can be able to  find the encrypted text on the consumer terminal

