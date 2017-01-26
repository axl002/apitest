import redis.clients.jedis.Jedis;

import java.util.*;

public class redisTest{
    public static void main(String[] args) {
        String itemClass = args[0];
        //Connecting to Redis server on localhost, syntax = string for local host, int for port num
        Jedis jedis = new Jedis("localhost", 30002);
        System.out.println("Connection to server sucessfully");
        //set the data in redis string

        // Get the stored data and print it
        //System.out.println(itemClass+ " "+jedis.lrange(itemClass, 0, 9));
        try {
            while(true){
                List<String> stuff = jedis.lrange(itemClass, 0, 10);
                for(String singleThing : stuff){
                    jedis.publish(itemClass, itemClass+ " "+ singleThing);
                    TimeUnit.SECONDS.sleep(1);
                }

            }
        }
        catch (Exception oops){
            oops.printStackTrace();
        }

    }

}