/**
 * Created by user on 1/23/17.
 */
package kafka;
import kafka.tools.ConsoleProducer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//import util.properties packages
import java.util.*;
import java.lang.*;

//import simple producer packages
//import kafka.javaapi.producer.Producer;
//import kafka.producer.*;
//import kafka.javaapi.*;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;


public class DankProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "broker1:9092,broker2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

        producer.close();
    }

//    public static void main(String[] args) {
//        long events = Long.parseLong(args[0]);
//        Random rnd = new Random();
//
//        Properties props = new Properties();
//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "SimplePartitioner");
//        props.put("request.required.acks", "1");
//
//        ProducerConfig config = new ProducerConfig(props);
//
//        Producer<String, String> producer = new KafkaProducer<String, String>(config);
//
//        for (long nEvents = 0; nEvents < events; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//        }
//        producer.close();
//    }
}
