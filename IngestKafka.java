
import java.io.*;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.*;
import java.lang.*;
import org.json.*;


//kafka imports

import kafka.tools.ConsoleProducer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//time imports
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class IngestKafka {

    static KafkaProducer<String, String> producer;
    static int reps = 0;
    static int numberOfQueryToGet = 3;
    static String startingKey = "0";
    static String theSource = "http://api.pathofexile.com/public-stash-tabs?id=";
    static String whereToDump = "testdump/";
    static String topic = "poeapi";
    public static void main(String[] args){


        //kafka config

        Properties props = new Properties();
        // THIS WORKS NOW
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("metadata.broker.list", "broker1:9092,broker2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 8000000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        // change this to a time stamp serializer
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);




        //dank config
        if (args.length >= 1){
            startingKey = args[0];
            numberOfQueryToGet = Integer.parseInt(args[1]);
        }
        try {
            //pullAndSave(theSource);
            pullMany(theSource, numberOfQueryToGet);
            //itemize();
            producer.close();
        }
        catch (Exception eeee){
            eeee.printStackTrace();
        }

    }


    private static void wrapProducerSend(String previousChange, String urlContent){

        // send time stamp as key
        // send api pull as value
        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        producer.send(new ProducerRecord<String, String>(topic, df.format(dateobj) +  previousChange, urlContent));

    }
    // make JSON object and extract array of user stashes (shops)
    public static void parseAndPrint(String jsonString){

        JSONObject allStash = new JSONObject(jsonString);
        JSONArray stashes = allStash.getJSONArray("stashes");

        // iterate through user stashes
        for (int i = 0; i<stashes.length();i++){
            String accountName = stashes.getJSONObject(i).getString("accountName");

            // iterate through item list
            JSONArray items = stashes.getJSONObject(i).getJSONArray("items");
            // print stash if it has items in it
            if (items.length()!= 0){
                System.out.println(accountName);
                System.out.println("\t"+items.length() + " item(s)");
                // don't print items without name (i.e. common rarity items, gems, currency items, etc)
                for(int j = 0; j<items.length();j++){
                    if(!items.getJSONObject(j).getString("name").equals("")){
                        System.out.println("\t\t" + items.getJSONObject(j).getString("name") + " "+items.getJSONObject(j).getString("typeLine"));
                    }
                }
            }
        }
    }


    // pull json and save to disk in txt file
    public static void pullAndSave(String theSource) throws Exception{
        URL url = new URL(theSource);
        BufferedReader br = wrapBR(url);

        String foobar = "";
        while(null != (foobar = br.readLine())) {
            System.out.println(foobar);

            BufferedWriter bw = new BufferedWriter(new FileWriter("sample.txt"));

            bw.write(foobar);
            bw.close();
        }
    }

    // wrapper for buffered reading of urls
    public static BufferedReader wrapBR(URL thingToWrap) throws Exception{
        return new BufferedReader(new InputStreamReader(thingToWrap.openStream()));
    }

    // wrapper for buffered reading of txt files
    public static BufferedReader wrapBR(FileReader thingToWrap) throws Exception{
        return new BufferedReader(thingToWrap);
    }

    // continuously query api until theLimit successful queries are reached and save results to disk
    public static void pullMany(String theSource, int theLimit) throws Exception{
        int theNum = 0;

        String nextChange = startingKey;
        String previousChange = "bar";
        String urlContent = "";
        while(theNum < theLimit) {
            TimeUnit.SECONDS.sleep(1);
            URL url = new URL(theSource + nextChange);
            System.out.println(url.toString());
            BufferedReader br = wrapBR(url);

            // System.out.println(urlContent);
            urlContent = br.readLine();
            if (null!= urlContent){

                JSONObject allStash = new JSONObject(urlContent);
                try {
                    nextChange = allStash.getString("next_change_id");
                }
                catch (Exception oopsNoKey) {
                    System.out.println("ooops invalid info pulled try again later...");
                    TimeUnit.SECONDS.sleep(10);
                }

                // System.out.println("-------------------------------------------------------------------------");
                // System.out.println("next:  " + nextChange);
                // System.out.println("prev: " + previousChange);
                // System.out.println("sleep 5 secs");


                // check if new key, if not there are no new items
                if(!nextChange.equals(previousChange)){
                    previousChange = nextChange;

                    // send producer
                    wrapProducerSend(previousChange, urlContent);
                    //BufferedWriter bw = new BufferedWriter(new FileWriter(whereToDump +previousChange + ".txt"));
                    //bw.write(urlContent);
                    //bw.close();
                    theNum++;
                }
                else {
                    // sleep 5 secs before checking again
                    System.out.println("no new items, wait current key is:");
                    System.out.println(previousChange);
                    TimeUnit.SECONDS.sleep(5);
                }
            }
        }
    }

    //extract item name, item id, item owner account name, item owner account id, item price, current time
    public static void itemize(int theLimit) throws Exception{

        // load each dump
        for(int i = 0; i < theLimit; i++){
            FileReader fr = new FileReader("dump" + Integer.toString(i) + ".txt");
            if(null != fr){
                BufferedReader br = wrapBR(fr);
                String foobar = "";
                while(null != (foobar = br.readLine())) {

                    parseAndPrint(foobar);

                }
            }
        }
    }

//	public static void testKafka(){
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 33554432);
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//		Producer<String, String> producer = new KafkaProducer<>(props);
//		for(int i = 0; i < 100; i++)
//			producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
//
//		producer.close();
//	}

}