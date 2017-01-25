javac -cp "Dankjson.jar:/usr/local/kafka/libs/*" IngestKafka.java
java -cp .:"/usr/local/kafka/libs/*:Dankjson.jar" IngestKafka '40232072-42849848-39877542-46082604-43225518' '7000'
