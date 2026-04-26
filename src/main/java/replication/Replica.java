package replication;

import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;


public class Replica {

    private final int     replicaId;
    private final Path    dataFile;
    private       Channel channel;

    public Replica(int replicaId) throws IOException {
        this.replicaId = replicaId;
        Path dir = Paths.get(RabbitConfig.REPLICA_DIR_PREFIX + replicaId);
        Files.createDirectories(dir);
        this.dataFile = dir.resolve(RabbitConfig.DATA_FILE_NAME);
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Replica <id>    e.g.  Replica 1");
            System.exit(1);
        }
        int id = Integer.parseInt(args[0]);
        System.out.println("[Replica " + id + "] Starting...");
        new Replica(id).start();
    }


    private void start() throws Exception {
        Connection connection = RabbitConfig.newConnection();
        channel = connection.createChannel();

        setupWriteConsumer();
        setupReadConsumer();

        System.out.println("[Replica " + replicaId + "] Ready.");
        System.out.println("[Replica " + replicaId + "] Data file : " + dataFile.toAbsolutePath());
        System.out.println("[Replica " + replicaId + "] Waiting for messages... (CTRL+C to stop)");

        Thread.currentThread().join();
    }

    
    private void setupWriteConsumer() throws IOException {
        channel.exchangeDeclare(RabbitConfig.WRITE_EXCHANGE, "fanout", true);

        String writeQueue = channel.queueDeclare().getQueue();
        channel.queueBind(writeQueue, RabbitConfig.WRITE_EXCHANGE, "");

        channel.basicConsume(writeQueue, true, (consumerTag, delivery) -> {
            String line = new String(delivery.getBody(), StandardCharsets.UTF_8);
            appendLine(line);
        }, consumerTag -> {});
    }

    private synchronized void appendLine(String line) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                dataFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {

            writer.write(line);
            writer.newLine();
            System.out.println("[Replica " + replicaId + "] Written  -> \"" + line + "\"");

        } catch (IOException e) {
            System.err.println("[Replica " + replicaId + "] ERROR writing: " + e.getMessage());
        }
    }

    
    private void setupReadConsumer() throws IOException {
        channel.exchangeDeclare(RabbitConfig.READ_EXCHANGE,  "direct", true);
        channel.exchangeDeclare(RabbitConfig.REPLY_EXCHANGE, "direct", true);

        String readQueue = RabbitConfig.READ_QUEUE_PREFIX + replicaId;
        channel.queueDeclare(readQueue, true, false, false, null);
        channel.queueBind(readQueue, RabbitConfig.READ_EXCHANGE, readQueue);

        channel.basicConsume(readQueue, true, (consumerTag, delivery) -> {
            String command    = new String(delivery.getBody(), StandardCharsets.UTF_8).trim();
            String replyQueue = delivery.getProperties().getReplyTo();

            System.out.println("[Replica " + replicaId + "] Received command: " + command);

            if (RabbitConfig.CMD_READ_LAST.equals(command)) {
                handleReadLast(replyQueue);
            } else if (RabbitConfig.CMD_READ_ALL.equals(command)) {
                handleReadAll(replyQueue);
            } else {
                System.err.println("[Replica " + replicaId + "] Unknown command: " + command);
            }
        }, consumerTag -> {});
    }

    private void handleReadLast(String replyQueue) throws IOException {
        String last = readLastLine();
        String response = "[Replica " + replicaId + "] Last line: " + last;
        publish(replyQueue, response);
        System.out.println("[Replica " + replicaId + "] Replied READ_LAST -> " + last);
    }

    private void handleReadAll(String replyQueue) throws IOException {
        if (!Files.exists(dataFile)) {
            publish(replyQueue, replicaId + "|" + RabbitConfig.CMD_END_OF_FILE);
            return;
        }
        List<String> lines = Files.readAllLines(dataFile, StandardCharsets.UTF_8);
        for (String line : lines) {
            publish(replyQueue, replicaId + "|" + line);
        }
        publish(replyQueue, replicaId + "|" + RabbitConfig.CMD_END_OF_FILE);
        System.out.println("[Replica " + replicaId + "] Replied READ_ALL (" + lines.size() + " lines sent)");
    }

    private void publish(String replyQueue, String message) throws IOException {
        channel.basicPublish(
                RabbitConfig.REPLY_EXCHANGE,
                replyQueue,
                null,
                message.getBytes(StandardCharsets.UTF_8)
        );
    }


    private synchronized String readLastLine() {
        if (!Files.exists(dataFile)) {
            return "(file does not exist yet)";
        }
        String last = "(file is empty)";
        try (BufferedReader reader = Files.newBufferedReader(dataFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) last = line;
            }
        } catch (IOException e) {
            last = "(error reading file)";
        }
        return last;
    }
}