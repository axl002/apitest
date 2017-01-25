/usr/local/kafka/bin/kafka-topics.sh --create --topic poeapi --replication-factor 3 --zookeeper localhost:2181 --partition 3

javac -cp "/usr/local/kafka/libs/*" ConsumeKafka.java
java -cp .:"/usr/local/kafka/libs/*" ConsumeKafka '/home/ubuntu/hugedump/'
