package replication;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class RabbitConfig {

    public static final String HOST = "localhost";

    public static final String WRITE_EXCHANGE = "write_exchange";
    public static final String READ_EXCHANGE  = "read_exchange";
    public static final String REPLY_EXCHANGE = "reply_exchange";

    
    public static final String READ_QUEUE_PREFIX  = "read_queue_";
    public static final String REPLY_QUEUE_PREFIX = "reply_queue_";

    public static final String CMD_READ_LAST   = "READ_LAST";
    public static final String CMD_READ_ALL    = "READ_ALL";

    public static final String CMD_END_OF_FILE = "EOF";

    public static final int NUM_REPLICAS = 3;

    
    public static final String REPLICA_DIR_PREFIX = "replica";
    public static final String DATA_FILE_NAME     = "data.txt";

    public static Connection newConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        return factory.newConnection();
    }
}