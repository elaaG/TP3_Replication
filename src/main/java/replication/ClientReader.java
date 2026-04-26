package replication;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;


public class ClientReader {

    private static final long TIMEOUT_MS = 5_000;

    public static void main(String[] args) throws Exception {

        try (Connection connection = RabbitConfig.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(RabbitConfig.READ_EXCHANGE,  "direct", true);
            channel.exchangeDeclare(RabbitConfig.REPLY_EXCHANGE, "direct", true);

            String replyQueue = channel.queueDeclare().getQueue();
            channel.queueBind(replyQueue, RabbitConfig.REPLY_EXCHANGE, replyQueue);

            BlockingQueue<String> firstReply = new ArrayBlockingQueue<>(1);

            channel.basicConsume(replyQueue, true, (consumerTag, delivery) -> {
                String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
                firstReply.offer(msg);   
            }, consumerTag -> {});

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .replyTo(replyQueue)
                    .build();

            byte[] body = RabbitConfig.CMD_READ_LAST.getBytes(StandardCharsets.UTF_8);

            int sent = 0;
            for (int i = 1; i <= RabbitConfig.NUM_REPLICAS; i++) {
                String targetQueue = RabbitConfig.READ_QUEUE_PREFIX + i;
                try {
                    channel.queueDeclarePassive(targetQueue); 
                    channel.basicPublish(RabbitConfig.READ_EXCHANGE, targetQueue, props, body);
                    System.out.println("[ClientReader] READ_LAST sent to Replica " + i);
                    sent++;
                } catch (Exception e) {
                    try { channel.close(); } catch (Exception ignored) {}
                    System.out.println("[ClientReader] Replica " + i + " not available (skipped).");
                }
            }

            if (sent == 0) {
                System.out.println("[ClientReader] No replicas reachable. Is RabbitMQ running?");
                return;
            }

            System.out.println("[ClientReader] Waiting for first reply (timeout " + TIMEOUT_MS + " ms)...");
            String reply = firstReply.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (reply == null) {
                System.out.println("[ClientReader] TIMEOUT: no replica responded within " + TIMEOUT_MS + " ms.");
            } else {
                System.out.println("[ClientReader] Response received:");
                System.out.println("  >> " + reply);
            }
        }
    }
}