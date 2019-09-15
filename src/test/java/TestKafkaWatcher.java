import com.google.common.io.Files;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeAll;
import topicWatchClient.*;

import static org.hamcrest.CoreMatchers.*;

public class TestKafkaWatcher {
  // The zookeeper server and kafka server host:ports
  static final String zooKeeperHostPort = "127.0.0.1:2181";
  static final String bootStrapServers = "127.0.0.1:9092";

  // The directory to store results from the topic callables
  @TempDir static  File TOPIC_EXEC_FOLDER = Files.createTempDir();
  @TempDir static  File KAFKA_FOLDER = Files.createTempDir();
  @TempDir static  File ZOOKEEPER_FOLDER = Files.createTempDir();

  static private KafkaLocal kafka;
  static private KafkaUtils kafkaAdmin;

  static private final int DEFAULT_TOPIC_PARTITIONS = 3;
  static private final short DEFAULT_TOPIC_REPLICATIONFACTOR = 1;

  // Initial topics upon initialization of Kafka
  static private final List<String> INITIAL_TOPICS =
      Arrays.asList("testTopic0", "testTopic1", "testTopic2");

  // Sample records to play around with
  static private final List<String> SAMPLE_RECORDS_0 =
          Arrays.asList("boob", "dee", "doo","tee");

  static final String GROUP_ID = "group0";
  static ModifiedTopicExec modifiedTopicExec;
  static NewTopicExec newTopicExec;
  static DeletedTopicExec deletedTopicExec;
  @BeforeAll
  public static void startKafka() throws IOException {

    // Set the modified, new, deleted watch callables
    //SampleKafkaExec sampleKafkaExec = new SampleKafkaExec(TOPIC_EXEC_FOLDER.getAbsolutePath());
    //FixMe: Replace for offcial testing
    SampleKafkaExec sampleKafkaExec = new SampleKafkaExec("/Users/jideanene/Work/training/kafka-beginners-course");
    modifiedTopicExec = sampleKafkaExec.modifiedTopicExec;
    newTopicExec = sampleKafkaExec.newTopicExec;
    deletedTopicExec = sampleKafkaExec.deletedTopicExec;

    // load the properties for kafka + zookeeper
    Properties kafkaProperties = TestingUtils.getKafkaProperties(KAFKA_FOLDER.getAbsolutePath());
    Properties zkProperties =
        TestingUtils.getZookeeperProperties(ZOOKEEPER_FOLDER.getAbsolutePath());

    try {
      // start kafka
      kafka = new KafkaLocal(kafkaProperties, zkProperties);
    } catch (Exception e) {
      System.out.println("Error running local Kafka broker");
      e.printStackTrace(System.out);
    }

    // create the admin client for kafka
    kafkaAdmin = new KafkaUtils();

    // Add a couple of topics
    INITIAL_TOPICS.forEach(
        (topicName) ->
            kafkaAdmin.createTopic(
                topicName, DEFAULT_TOPIC_PARTITIONS, DEFAULT_TOPIC_REPLICATIONFACTOR));

    // add a couple records to a topic0
      TestingTopicProducer.produceData(INITIAL_TOPICS.get(0),SAMPLE_RECORDS_0);


    // Launch watcher
    // verify output on initialization.
  }

  @AfterAll
  public static void stopKafka() {
    kafka.stop();
  }

  @Test
  public void testKafkaWatcher() throws InterruptedException, ExecutionException {

    // records to watch
    final List<String> sampleRecords =
              Arrays.asList("hell", "or", "high","water");


    // Create the Kafka Watcher Object and launch it
    KafkaWatcher kafkaWatcher =
            new KafkaWatcher.Builder(zooKeeperHostPort, bootStrapServers,GROUP_ID)
                    .setDeletedTopicExec(deletedTopicExec)
                    .setModifiedTopicExec(modifiedTopicExec)
                    .setNewTopicExec(newTopicExec)
                    .build();
    kafkaWatcher.run();

    /*
          // Run the watcher synchronously
          kafkaWatcher.run();
          // returns a kafka future
          ListTopicsResult listTopicsResult = kafkaAdmin.listTopics();
          Set<String> topicNames = listTopicsResult.names().get();
          System.out.println(topicNames);
    */
    // Launch watcher

    // Since this is first time launching verify that the topic we added a couple records to
    // is registered by the modification listener

    // Create topics
    // verify watcher catches new topic

    // Delete a couple topic
    // verify watcher catches deleted topic

    // Add records to existing topics
    // verify watcher catches these additions

  }
}
