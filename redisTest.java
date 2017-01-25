import redis.clients.jedis.Jedis;
import java.util.Random;

public class redisTest {
    public static void main(String[] args) {
        //Connecting to Redis server on localhost, syntax = string for local host, int for port num
        Jedis jedis = new Jedis("localhost", 30001);
        System.out.println("Connection to server sucessfully");
        //set the data in redis string
        Random rand = new Random();

        for (int i = 0; i < 15;i++){
            int  n = rand.nextInt(50) + 1;
            System.out.println(n);
            jedis.lpush("price", Integer.toString(n));
        }
        // Get the stored data and print it
        System.out.println("Stored string in redis:: "+ jedis.lpop("price"));
    }
}