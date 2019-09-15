package topicWatchClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Utility functions for Kafka
 * Based on @see <a href="http://community.nuxeo.com/api/nuxeo/release-9.10/javadoc/src-html/org/nuxeo/lib/stream/log/kafka/KafkaUtils.html">Nuxeo Implementation of KafkaUtils</a>
 */
public class KafkaUtils implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    protected final AdminClient adminClient;

    public KafkaUtils(){
      this(getDefaultAdminProperties());
    }
    public KafkaUtils(Properties adminProperties){
        this.adminClient = AdminClient.create(adminProperties);
    }

    public static Properties getDefaultAdminProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        return props;
    }

    /**
     * Checks if a kafka topic exists
     * @param topic kafka topic
     * */
    public boolean topicExists(String topic) {
        return partitions(topic) > 0;
    }

    /**
     * Returns the number of partitions for a topic
     * @param topic kafka topic
     * */
    public int partitions(String topic) {
        try {
            TopicDescription desc = adminClient.describeTopics(Collections.singletonList(topic))
                    .values()
                    .get(topic)
                    .get();
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Topic %s exists: %s", topic, desc));
            }
            return desc.partitions().size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return -1;
            }
            throw new IllegalStateException(e);
        }
    }

    /**
     *
     * @param topic Kafka topic
     * @param partitions number of kafka partitions for the topic
     * @param replicationFactor number of replications for the topic
     */
    public void createTopic(String topic, int partitions, short replicationFactor) {
        LOG.info("Creating topic: " + topic + ", partitions: " + partitions + ", replications: " + replicationFactor);
        if (topicExists(topic)) {
            throw new IllegalArgumentException("Cannot create Topic already exists: " + topic);
        }
        CreateTopicsResult ret = adminClient.createTopics(
                Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)));
        try {
            ret.all().get(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unable to create topics " + topic + " within the timeout", e);
        }
    }

    /**
     * Delete a single consumer group
     *
     * @param groupID the groupID of the consumer
     * @return DeleteConsumerGroupsResult
     */
    public DeleteConsumerGroupsResult deleteConsumer(String groupID){
        return deleteConsumerGroups(Arrays.asList(groupID));
    }

    /**
     * Delete a collection of consumer groups
     *
     * @param groupIDs collection of consumer group IDs
     * @return DeleteConsumerGroupsResult
     * */
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIDs){
        return adminClient.deleteConsumerGroups(groupIDs);

    }

    /**
     * List the topics available in the cluster with the default options.
     *
     * This is a convenience method for #{@link AdminClient#listTopics(ListTopicsOptions)} with default options.
     *
     * @return                  The ListTopicsResult.
     */
    public ListTopicsResult listTopics() {
        return adminClient.listTopics(new ListTopicsOptions());
    }

    /**
     * Checks if a groupID already exists or not
     *
     * @param groupID the groupID we are checking existence of
     * @return true if the groupID exists in Kafka otherwise false
     * @deprecated Not useful at the moment
     */
    private boolean hasConsumerGroupID(String groupID) {
        return listAllConsumers().contains(groupID);
    }

    /**
     * List all consumerID associated to the bootStrapServers FIXME: Think about if the set data
     * structure is appropriate

     * @return a set of all kafka consumers
     */
    private synchronized Set<String> listAllConsumers() {
        Set<String> allConsumers;
        try {
            allConsumers =
                    this.adminClient.listConsumerGroups().all().get().stream()
                            .map(ConsumerGroupListing::groupId)
                            .collect(Collectors.toSet());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
        return allConsumers;
    }

    @Override
    public void close(){
        adminClient.close();
        LOG.info("[KafkaUtils] closed");
    }

}
