import redis.clients.jedis.Jedis;


public class redisTest {
    public static void main(String[] args) {
        String itemClass = args[0];
        //Connecting to Redis server on localhost, syntax = string for local host, int for port num
        Jedis jedis = new Jedis("localhost", 30001);
        System.out.println("Connection to server sucessfully");
        //set the data in redis string

        // Get the stored data and print it
        System.out.println(itemclass+ " "+jedis.lrange(itemClass, 1, 10));
    }
}