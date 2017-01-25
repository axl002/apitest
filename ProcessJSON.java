/**
 * Created by user on 1/24/17.
 */

import java.io.*;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.*;
import java.lang.*;
import org.json.*;


//kafka imports
//
//import kafka.tools.ConsoleProducer.ProducerConfig;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;

//time imports
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
public class ProcessJSON {

    public static void main(String[] args){
        String whereFind = "/home/ubuntu/hugedump/";
        int theLimit = 5;
        try {
            File dir = new File(whereFind);
            File[] listing = dir.listFiles();

            if (listing != null){
                System.out.println(listing.length);
                for(File child : listing){
                    itemize(theLimit, child);
                }
            }
        }catch (Exception e) {

        }

    }

    //extract item name, item id, item owner account name, item owner account id, item price, current time
    public static void itemize(int theLimit, File child) throws Exception{

        // load each dump
        for(int i = 0; i < theLimit; i++){
            FileReader fr = new FileReader(child);
            if(null != fr){
                BufferedReader br = wrapBR(fr);
                String foobar = "";
                while(null != (foobar = br.readLine())) {

                    parseAndPrint(foobar);

                }
            }
        }
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
                        System.out.println("\t\t" + items.getJSONObject(j).getString("price"));
                    }
                }
            }
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

}
