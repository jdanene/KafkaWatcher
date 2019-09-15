import kafka.server.KafkaConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import kafka.server.KafkaServerStartable;

/**
 * A local Kafka server for running unit tests.
 * Reference: https://gist.github.com/fjavieralba/7930018/
 */
public class TestingUtils {

    /**
     * Get the default properties for a kafka server
     *
     * @param logDir ideally a temporary directory path.
     * */
    public static Properties getKafkaProperties(String logDir) {
        Properties properties = new Properties();

        properties.put("broker.id","0");
        //properties.put("num.network.threads","3");
        //properties.put("num.io.threads","8");
        properties.put("socket.send.buffer.bytes","8102400");
        properties.put("socket.receive.buffer.bytes","102400");
        properties.put("socket.request.max.bytes","104857600");
        properties.put("log.dirs",logDir);
        properties.put("num.partitions","3");
        //properties.put("num.recovery.threads.per.data.dir","1");
        properties.put("offsets.topic.replication.factor","1");
        properties.put("transaction.state.log.replication.factor","1");
        properties.put("transaction.state.log.min.isr","1");
        properties.put("log.retention.hours","168");
        properties.put("log.segment.bytes","1073741824");
        properties.put("log.retention.check.interval.ms","300000");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("zookeeper.connection.timeout.ms","6000");
        properties.put("group.initial.rebalance.delay.ms","0");
        properties.put("delete.topic.enable", "true");
        return properties;
    }

    /**
     * Get default properties for a zookeeper server
     *
     * @param zookeeperDir ideally a temporary directory path
     * */
    public static Properties getZookeeperProperties(String zookeeperDir) {
        Properties properties = new Properties();
        properties.put("clientPort", "2181");
        properties.put("dataDir", zookeeperDir);
        properties.put("maxClientCnxns", "0");
        return properties;
    }

    static private synchronized int getNextPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
