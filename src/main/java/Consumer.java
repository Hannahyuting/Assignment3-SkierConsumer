import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.swagger.client.model.LiftRide;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {

    private static final String QUEUE_NAME = "skiersInfo";
    private static final String SERVER = "35.165.112.73";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(SERVER);
        factory.setUsername("yutingz");
        factory.setPassword("yutingz");
        factory.setVirtualHost("vhost");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        JedisPool pool = new JedisPool(config, "52.38.199.238", 6379);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    channel.basicQos(1);

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        JSONObject jsonObject = new JSONObject(message);
                        Gson gson = new Gson();
                        int resortId = jsonObject.getInt("resortId");
                        int seasonId = jsonObject.getInt("seasonId");
                        int dayId = jsonObject.getInt("dayId");
                        int skierId = jsonObject.getInt("skierId");
                        LiftRide liftRide = gson.fromJson(jsonObject.get("liftRide").toString(), LiftRide.class);
                        int time = liftRide.getTime();
                        int liftID = liftRide.getLiftID();

                        String skierIdValue = Integer.toString(skierId);

                        try (Jedis jedis = pool.getResource()) {
                            jedis.hset(skierIdValue, "resortId", Integer.toString(resortId));
                            jedis.hset(skierIdValue, "seasonId", Integer.toString(seasonId));
                            jedis.hset(skierIdValue, "dayId", Integer.toString(dayId));
                            jedis.hset(skierIdValue, "time", Integer.toString(time));
                            jedis.hset(skierIdValue, "liftID", Integer.toString(liftID));
                        }
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    };
                    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        for (int i = 0; i < 500; i++) {
            Thread thread = new Thread(runnable);
            thread.start();
        }
    }
}
