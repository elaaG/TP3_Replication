package replication;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class ClientReaderV2 {

    private static final long TIMEOUT_MS = 8_000;

    public static void main(String[] args) throws Exception {

        Map<Integer, List<String>> replicaLines    = new ConcurrentHashMap<>();
        Set<Integer>               finishedReplicas = ConcurrentHashMap.newKeySet();

        try (Connection connection = RabbitConfig.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(RabbitConfig.READ_EXCHANGE,  "direct", true);
            channel.exchangeDeclare(RabbitConfig.REPLY_EXCHANGE, "direct", true);

            String replyQueue = channel.queueDeclare().getQueue();
            channel.queueBind(replyQueue, RabbitConfig.REPLY_EXCHANGE, replyQueue);

            List<Integer> liveIds = new ArrayList<>();
            for (int i = 1; i <= RabbitConfig.NUM_REPLICAS; i++) {
                try {
                    channel.queueDeclarePassive(RabbitConfig.READ_QUEUE_PREFIX + i);
                    liveIds.add(i);
                } catch (Exception e) {
                    System.out.println("[ClientReaderV2] Replica " + i + " not reachable ");
                }
            }

            if (liveIds.isEmpty()) {
                System.out.println("[ClientReaderV2] No replicas reachable ");
                return;
            }

            System.out.println("[ClientReaderV2] Live replicas: " + liveIds);

            CountDownLatch latch = new CountDownLatch(liveIds.size());

            channel.basicConsume(replyQueue, true, (consumerTag, delivery) -> {
                String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
                int sep = raw.indexOf('|');
                if (sep < 0) return;

                int    id      = Integer.parseInt(raw.substring(0, sep));
                String content = raw.substring(sep + 1);

                if (RabbitConfig.CMD_END_OF_FILE.equals(content)) {
                    if (finishedReplicas.add(id)) {   // count each replica once only
                        latch.countDown();
                        System.out.println("[ClientReaderV2] Replica " + id + " finished streaming ");
                    }
                } else {
                    replicaLines.computeIfAbsent(id, k -> new CopyOnWriteArrayList<>()).add(content);
                }
            }, consumerTag -> {});

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .replyTo(replyQueue)
                    .build();
            byte[] body = RabbitConfig.CMD_READ_ALL.getBytes(StandardCharsets.UTF_8);

            for (int id : liveIds) {
                channel.basicPublish(
                        RabbitConfig.READ_EXCHANGE,
                        RabbitConfig.READ_QUEUE_PREFIX + id,
                        props,
                        body
                );
                System.out.println("[ClientReaderV2] READ_ALL sent to Replica " + id);
            }

            boolean allDone = latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!allDone) {
                System.out.println("[ClientReaderV2] WARNING: timeout before all replicas finished ");
            }

            int majority = (liveIds.size() / 2) + 1;   
            System.out.println("\n[ClientReaderV2] Majority threshold: " + majority
                    + " out of " + liveIds.size() + " replicas.\n");

            Map<String, Integer> lineVotes = new LinkedHashMap<>();
            for (Map.Entry<Integer, List<String>> entry : replicaLines.entrySet()) {
                Set<String> uniqueLines = new LinkedHashSet<>(entry.getValue());
                for (String line : uniqueLines) {
                    lineVotes.merge(line, 1, Integer::sum);
                }
            }

            List<String> result = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : lineVotes.entrySet()) {
                if (entry.getValue() >= majority) {
                    result.add(entry.getKey());
                }
            }

            result.sort(Comparator.comparingInt(ClientReaderV2::lineNumber));

            System.out.println("  CONSISTENT CONTENT     ");
            if (result.isEmpty()) {
                System.out.println(" no lines passed the majority threshold ");
            } else {
                for (String line : result) {
                    System.out.println("  " + line);
                }
            }
            System.out.println("  Total lines : " + result.size());

            System.out.println("Raw content per replica n");
            for (int i = 1; i <= RabbitConfig.NUM_REPLICAS; i++) {
                List<String> lines = replicaLines.getOrDefault(i, List.of());
                if (lines.isEmpty()) {
                    System.out.println("  Replica " + i + ": (offline or empty)");
                } else {
                    System.out.println("  Replica " + i + " (" + lines.size() + " lines): " + lines);
                }
            }
        }
    }

    private static int lineNumber(String line) {
        try {
            return Integer.parseInt(line.trim().split("\\s+")[0]);
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE;
        }
    }
}
