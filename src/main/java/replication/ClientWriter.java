package replication;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class ClientWriter {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: ClientWriter \"<lineNumber> <text>\"");
            System.err.println("Example: ClientWriter \"1 Texte message1\"");
            System.exit(1);
        }

        String line = args[0];

        try (Connection connection = RabbitConfig.newConnection();
             Channel channel = connection.createChannel()) {

           
            channel.exchangeDeclare(RabbitConfig.WRITE_EXCHANGE, "fanout", true);

            channel.basicPublish(
                    RabbitConfig.WRITE_EXCHANGE,
                    "",     
                    null,
                    line.getBytes("UTF-8")
            );

            System.out.println("[ClientWriter] Sent line -> \"" + line + "\"");
        }
    }
}