/**
 * Created by user on 1/24/17.
 */

import java.util.Properties;
import java.util.Arrays;
import java.io.*;
import java.util.concurrent.TimeUnit;

import org.json.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

//redisStuff
import redis.clients.jedis.Jedis;
import java.util.Random;

public class ConsumeKafka {

    static String whereToDump = "testdump/";
    public static void main(String[] args) throws Exception {

        if (args.length > 0){
            whereToDump = args[0];
        }
        String topic = "poeapi";
        //String group = args[1].toString();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "zookeeper"); // need to test if zookeeper is required group, it works but do other groups work?
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("fetch.message.max.bytes","10000000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);

        //redis connect
        Jedis jedis = new Jedis("localhost", 30001);
        System.out.println("Connection to server sucessfully");

        Random rand = new Random();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
//                double curOffset = record.offset();
//                String curKey = record.key();
//                String curVal = record.value();
                JSONObject item = new JSONObject(record.value());
                String testString = "nothing here";
                try {
                    testString = item.getString("name")+" "+item.getString("typeLine") +"\n" + item.getString("owner");
                    //System.out.println(testString);
                    try {
                        String price = item.getString("note");
                        int  n = rand.nextInt(50) + 1;
                        jedis.put(item.getString("name")+" "+item.getString("typeLine"), Integer.toString(n));
                    }
                    catch (Exception noNote) {
                        // no note means no price means not on sale

                    }
                    // push to redis



                }
                catch (Exception oopsNoKey) {
                    System.out.println("ooops invalid info pulled try again later...");
                    //TimeUnit.SECONDS.sleep(10);
                }


//                BufferedWriter bw = new BufferedWriter(new FileWriter(whereToDump +record.key() + ".txt"));
//                bw.write(record.value());
//                bw.close();
                //System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), testString);
            }



        }
    }

}
