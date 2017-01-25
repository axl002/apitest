/**
 * Created by user on 1/24/17.
 */

import java.util.Properties;
import java.util.Arrays;
import java.io.*
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumeKafka {

    static String whereToDump = "testdump/";
    public static void main(String[] args) throws Exception {

        if (args.length > 0){
            whereToDump = args[0]
        }
        String topic = "my-topic";
        //String group = args[1].toString();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "zookeeper"); // need to test if zookeeper is required group, it works but do other groups work?
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
//                double curOffset = record.offset();
//                String curKey = record.key();
//                String curVal = record.value();

                BufferedWriter bw = new BufferedWriter(new FileWriter(whereToDump +record.key() + ".txt"));
                bw.write(record.value());
                bw.close();
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }



        }
    }

}
