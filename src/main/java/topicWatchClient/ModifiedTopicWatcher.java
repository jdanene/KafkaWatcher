package topicWatchClient;

import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * `ModifiedTopicWatcher` is a subclass of {@link KafkaWatcher} and implements the Runnable class.
 * It watches for modifications by launching a kafka topic consumer on a topic, and continuously
 * poll the topic for new data records.
 *
 * <p>TODO: Figure out what happens w/ multiple servers TODO: Set security so that new consumer
 * can't be added to existing groupID after already started. Also only key holder can delete
 * consumer, so set security on groupID.
 */
class ModifiedTopicWatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ModifiedTopicWatcher.class);

  // uniquely identifies`ModifiedTopicWatcher' and the Kafka consumer group `ModifiedTopicWatcher'
  // is managing
  private String groupID;
  // host:port of the bootstrap servers (kafka servers)
  private String bootStrapServers;

  // the callable executed each time an existing topic receives kafka messages
  private ModifiedTopicExec modifiedTopicExec;

  // timeout duration of kafka topic polling, used by the consumer thread instances
  private Duration timeOut;

  // concurrency tool that allows for variable number of threads
  private final Phaser PHASER = new Phaser(1);
  private Integer PHASE = PHASER.getPhase();

  // balances new and deleted topics amongst the consumer instances that `ModifiedTopicWatcher' is
  // managing
  private LoadBalancer LOAD_BALANCER;
  // the number of consumer instance threads that will be launched by `ModifiedTopicWatcher'
  private int NUMBER_OF_CONSUMER_THREADS;

  // the set of consumer instances runnable's
  private Map<String, ConsumerRunnable> mapOfConsumerInstances = new HashMap<>();

  // signalling bits & buses
  private boolean isDead = false;
  private boolean newTopicSignal = false;
  private boolean deletedTopicSignal = false;
  private Collection<String> newTopicInputData;
  private Collection<String> deletedTopicInputData;

  private ModifiedTopicWatcher(String groupID) {
    this.groupID = groupID;
    System.out.println(
        "\t[ModifiedTopicWatcher] ModifiedTopicWatcher initialized for consumer group: " + groupID);
    LOAD_BALANCER = new LoadBalancer();
  }

  /* *******************************************************
                     Public Methods
  *********************************************************/

  /** Call this to run the `ModifiedTopicWatcher' */
  @Override
  public void run() {
    System.out.println(
        "\t[ModifiedTopicWatcher#run()] ModifiedTopicWatcher running for consumer group: "
            + groupID);
    synchronized (this) {
      while (!(isDead)) {
        try {
          PHASER.awaitAdvanceInterruptibly(PHASE);
        } catch (InterruptedException e) {

          // shutdown if did not receive newTopicSignal or deletedTopicSignal
          if (!(newTopicSignal || deletedTopicSignal)) {
            _shutdown();
            Thread.currentThread().interrupt();
          }

          // run addTopics on `newTopicInputData`
          if (newTopicSignal) {
            System.out.println("\t[ModifiedTopicWatcher#run()] New topics detected in consumer group: "+groupID );
            _addTopic(newTopicInputData);
            newTopicSignal = false;
          }

          // run removeTopic on `newTopicInputData`
          if (deletedTopicSignal) {
            System.out.println("\t[ModifiedTopicWatcher#run()] Deleted topics detected in consumer group: "+groupID);
            _removeTopic(deletedTopicInputData);
            deletedTopicSignal = false;
          }
        }
      }
    }
  }

  public static void shutdown(Thread modifiedTopicWatcherThread) {
    modifiedTopicWatcherThread.interrupt();
  }

  public static void addTopic(
      Collection<String> topicSet,
      ModifiedTopicWatcher modifiedTopicWatcher,
      Thread modifiedTopicWatcherThread) {

    modifiedTopicWatcher.newTopicSignal = true;
    modifiedTopicWatcher.newTopicInputData = topicSet;
    modifiedTopicWatcherThread.interrupt();
  }

  public static void removeTopic(
      Collection<String> topicSet,
      ModifiedTopicWatcher modifiedTopicWatcher,
      Thread modifiedTopicWatcherThread) {

    modifiedTopicWatcher.deletedTopicSignal = true;
    modifiedTopicWatcher.deletedTopicInputData = topicSet;
    modifiedTopicWatcherThread.interrupt();
  }

  /**
   * Adds a topic name to be watched
   *
   * @param topicName a topic name to watch
   */
  private void _addTopic(String topicName) {
    int consumersNeeded = NUMBER_OF_CONSUMER_THREADS - LOAD_BALANCER.getNumberOfWorkers();
    if (consumersNeeded != 0) {

      // generate the consumer
      generateConsumer(topicName);
    } else {
      // add topic to load balancer
      LoadBalancer.Worker worker = LOAD_BALANCER.addJob(topicName);
      // update topic sets of consumer whose set has changed
      updateTopicSet(worker);
    }
    System.out.println(
        "\t[ModifiedTopicWatcher#addTopic(String)] Added topic "
            + topicName
            + " to consumer group: "
            + groupID);
  }

  /**
   * Adds a set of topic names to be watched
   *
   * @param topicSet a set of topic name to watch
   */
  private synchronized void _addTopic(Collection<String> topicSet) {
    System.out.println(
        "\t[ModifiedTopicWatcher#addTopic(String)] Consumer group "
            + groupID
            + " added topic set:"
            + topicSet);

    Collection<String> adjustedTopicSet = new HashSet<>();
    int consumersNeeded = NUMBER_OF_CONSUMER_THREADS - LOAD_BALANCER.getNumberOfWorkers();

    if (consumersNeeded != 0) {
      // find the number of consumer instance threads needed
      int consumerToGenerate;
      if (topicSet.size() > consumersNeeded) {
        consumerToGenerate = consumersNeeded;
      } else {
        consumerToGenerate = topicSet.size();
      }

      // generate the consumer instance threads needed
      int i = 0;
      for (String topicName : topicSet) {
        if (i < consumerToGenerate) {
          generateConsumer(topicName);
        } else {
          adjustedTopicSet.add(topicName);
        }
        i++;
      }
    } else {
      adjustedTopicSet.addAll(topicSet);
    }

    // add topics to load balancer
    Set<LoadBalancer.Worker> consumer = LOAD_BALANCER.addJob(adjustedTopicSet);
    // update topic sets of consumerRunnables
    consumer.forEach((this::updateTopicSet));
  }

  /**
   * A topic name to be deleted
   *
   * @param topicName a topic name to deleted
   */
  private void _removeTopic(String topicName) {
    // remove the topicName from LOAD_BALANCER
    Set<LoadBalancer.Worker> setOfWorkers = LOAD_BALANCER.removeJob(topicName);

    for (LoadBalancer.Worker consumer : setOfWorkers) {
      // if the topicName caused a consumerInstance to have zero topics then shutdown it down
      if (consumer.getNumberOfJobs() == 0) {
        shutdownConsumer(consumer);
      } else {
        // update topicSet of consumer
        updateTopicSet(consumer);
      }
    }

    System.out.println(
        "\t[ModifiedTopicWatcher#removeTopic(String)] Removed topic "
            + topicName
            + " from consumer group: "
            + groupID);
  }

  /**
   * A set of topic name to be deleted
   *
   * @param topicSet a topic name to deleted
   */
  private synchronized void _removeTopic(Collection<String> topicSet) {
    Set<Set<LoadBalancer.Worker>> setOfSetsOfWorkers = LOAD_BALANCER.removeJob(topicSet);

    setOfSetsOfWorkers.forEach(
        (setOfWorkers) ->
            setOfWorkers.forEach(
                ((consumer) -> {
                  if (consumer.getNumberOfJobs() == 0) {
                    shutdownConsumer(consumer);
                  } else {
                    updateTopicSet(consumer);
                  }
                })));

    System.out.println(
        "\t[ModifiedTopicWatcher#removeTopic(String)] Removed topic "
            + topicSet
            + " from consumer group: "
            + groupID);
  }

  /**
   * Call this to shutdown the instance of ModifiedTopicWatcher -- this shutdown all active
   * consumerThreads.
   */
  private void _shutdown() {
    System.out.println(
        "\t[ModifiedTopicWatcher] Shutting consumer group: "
            + groupID);
    // shutdown the thread pool
    shutdownAllModificationListeners();
    // the main thread
    PHASER.arriveAndDeregister();
    // set the dead bit
    isDead = true;
  }

  /* *******************************************************
                   Private Methods
  ********************************************************/
  /**
   * Generates a new consumerInstance thread. Consumers need to be generated with at least one topic
   * name
   *
   * @param topicName the topicName
   */
  private void generateConsumer(String topicName) {
    // generate consumerID for new consumer
    String newConsumerID = generateConsumerID();

    // add topic and new consumer to LOAD_BALANCER
    Set<LoadBalancer.Worker> adjustedWorkerSet = LOAD_BALANCER.addWorker(newConsumerID);
    LOAD_BALANCER.addJob(topicName);

    // launch the consumer thread
    LoadBalancer.Worker newConsumer = LOAD_BALANCER.getWorker(newConsumerID);
    launchConsumer(newConsumer.getWorkerID(), newConsumer.getJobSet());

    // update the topic set for all the consumer who had their topicSets changed.
    adjustedWorkerSet.forEach(this::updateTopicSet);
  }

  /**
   * Launches a consumerInstance thread giving it the `consumerID` identifier and `topicSet' to
   * watch
   *
   * @param consumerID identifier of consumerInstance
   * @param topicSet set of topics consumerInstance is watching
   */
  private void launchConsumer(String consumerID, Set<String> topicSet) {
    // register the thread - equiv to counting up a latch
    PHASER.register();

    // create consumerRunnable
    ConsumerRunnable consumerRunnable =
        new ConsumerRunnable(bootStrapServers, groupID, consumerID, topicSet, PHASER);

    // add entry to the map
    mapOfConsumerInstances.put(consumerID, consumerRunnable);

    // add thread to thread pool handled by the PHASER
    Thread consumerThread = new Thread(consumerRunnable);
    consumerThread.start();
    System.out.printf(
        "\t[ModifiedTopicWatcher#launchConsumer()] New consumer thread launched: %d/%d%n",
        LOAD_BALANCER.getNumberOfWorkers(), NUMBER_OF_CONSUMER_THREADS);
  }

  /**
   * Updates a topic set of a consumerInstance
   *
   * @param consumer Worker with the consumerID and topicSet to update consumerInstance with
   */
  private void updateTopicSet(LoadBalancer.Worker consumer) {
    ConsumerRunnable consumerRunnable = mapOfConsumerInstances.get(consumer.getWorkerID());
    updateConsumerTopicSet(consumer.getJobSet(), consumerRunnable);
  }

  /**
   * Shuts down a consumer instance by
   *
   * <ul>
   *   <li>shutting down the consumerRunnable thread
   *   <li>removing the consumerID from `mapOfConsumerInstances'
   *   <li>removing the consumer from `LOAD_BALANCER'
   * </ul>
   *
   * @param consumer the consumer to shutdown
   */
  private void shutdownConsumer(LoadBalancer.Worker consumer) {
    // shutdown consumerInstance
    ConsumerRunnable consumerRunnable = mapOfConsumerInstances.get(consumer.getWorkerID());
    consumerRunnable.shutdown();
    // remove consumer from mapOfConsumerInstances
    mapOfConsumerInstances.remove(consumer.getWorkerID());
    // remove consumer from LOAD_BALANCER
    LOAD_BALANCER.removeWorker(consumer.getWorkerID());

    System.out.println("\t[ModifiedTopicWatcher#shutdownConsumer()]shutdown of a consumer thread in consumer group: "+groupID);
  }

  /** Returns a unique consumerID relative to ModifiedTopicWatcher instance */
  private String generateConsumerID() {
    return "consumer" + (num++);
  }

  private int num = 0;

  /** Launches code to stop a consumerRunnable */
  private void stopListeningInOnModifications(ConsumerRunnable consumerRunnable) {
    consumerRunnable.shutdown();
  }

  /** Shutdown all consumerRunnable's that `ModifiedTopicWatcher` is managing. * */
  private void shutdownAllModificationListeners() {
    mapOfConsumerInstances.values().forEach(this::stopListeningInOnModifications);
  }

  /**
   * Sets the properties for kafka consumer
   *
   * @param bootstrapServers The comma separated list of port:host kafka s
   * @param groupID The group ID
   */
  private Properties getProperties(String bootstrapServers, String groupID) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);

    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return properties;
  }

  /**
   * Notify the consumer instance that the topicSet has changed then interrupt the polling so that
   * the kafka consumer can listen to the new topicSet
   *
   * @param topicSet a set of kafka topic names for the consumer to watch
   */
  private static void updateConsumerTopicSet(
      Set<String> topicSet, ConsumerRunnable consumerRunnable) {
    consumerRunnable.topicSet = topicSet;
    consumerRunnable.hasTopicSetChanged = true;
    consumerRunnable.consumer.wakeup();
  }

  /** The Runnable that actually watches the topic of interest. Threading is done via latches */
  private class ConsumerRunnable implements Runnable {

    private final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);
    // kafka consumer that this `ConsumerRunnable` is managing - this is the thing actually watching
    // for topics
    private KafkaConsumer<String, String> consumer;

    /* **********************************************************************************************
     * boolean that when set to true ConsumerRunnable is interrupted and a new topic set is watched,
     * and then the boolean is set to false again.
     * **********************************************************************************************/
    private volatile boolean hasTopicSetChanged = false;
    // the set of topics the ConsumerRunnable is watching
    private Set<String> topicSet;
    // latch used for concurrency
    private Phaser phaser;
    // unique id of ConsumerRunnable instance
    private String consumerID;

    private ConsumerRunnable(
        String bootstrapServers,
        String groupID,
        String consumerID,
        Set<String> topicSet,
        Phaser phaser) {

      // set the unique ID of ConsumerRunnable instance
      this.consumerID = consumerID;

      // set the topicSet
      this.topicSet = topicSet;

      // countdown latch for multi-threading
      this.phaser = phaser;

      // consumer configurations
      Properties properties = getProperties(bootstrapServers, groupID);

      // creating kafka consumer
      consumer = new KafkaConsumer<>(properties);

      // subscribing consumer to our topic(s)
      consumer.subscribe(topicSet);
    }

    /**
     * Polls a given consumer for new data to the topic it is subscribed to and call
     * `modifiedTopicExec()' on new data.
     *
     * @param consumer the kafka consumer that is wathcing the topic
     * @exception WakeupException thrown when Consumer.wakeup() is called in {@link
     *     ConsumerRunnable#shutdown()}
     */
    private void pollTopicAndExec(KafkaConsumer<String, String> consumer) throws WakeupException {

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeOut);
        try {
          // run the passed in function on the polled records
          modifiedTopicExec.onStateChange(records);
        } catch (Exception e) {
          // FixMe: Handle this correctly
          LOG.error(
              "\t[ModifiedTopicWatcher#pollTopicAndExec()] internal error for the user defined `modifiedTopicExec' ",
              e);
        }
      }
    }

    /** Shut downs consumer thread */
    void shutdown() {
      // Consumer.wakeup() is a special method to interrupt Consumer.poll()
      // throwing the exception `WakeUpException'
      consumer.wakeup();
    }

    /** Overrides Runnable.run() */
    @Override
    public void run() {
      try {
        pollTopicAndExec(consumer);
      } catch (WakeupException e) {

        if (hasTopicSetChanged) {
          // subscribe to the updated topic set
          consumer.unsubscribe();
          consumer.subscribe(topicSet);
          // reset the boolean to false
          hasTopicSetChanged = false;
          // run again
          run();
        } else {
          System.out.println(
              "\t[ModifiedTopicWatcher#ConsumerRunnable] Received shutdown signal in consumer group: "
                  + groupID);
          // close consumer when done
          consumer.close();
          // tells main code we are able to exit - (equiv to countDown)
          phaser.arriveAndDeregister();
        }
      } finally {
        System.out.println(
            "\t[ModifiedTopicWatcher#ConsumerRunnable] consumer runnable shut down: "
                + LOAD_BALANCER.getNumberOfWorkers()
                + "/"
                + NUMBER_OF_CONSUMER_THREADS
                + "threads left in consumer group "
                + groupID);
      }
    }

    /** Gets the consumerID */
    String getConsumerID() {
      return consumerID;
    }

    /**
     * Since we are overriding Object.equals() we must override Object.hashCode() -- hashCode must
     * generate equal values for equal objects.
     *
     * @return hashcode used for equality comparison
     */
    @Override
    public int hashCode() {
      return consumerID.hashCode();
    }

    @Override
    public String toString() {
      return "{consumerID: " + consumerID + ", topicSet:" + topicSet + "}";
    }

    /**
     * Overrides Object.equals() to be based on consumerID since consumerIDs are unique to a
     * ConsumerRunnable
     *
     * @param obj instance of ConsumerRunnable to compare
     * @return boolean signifying equality
     */
    @Override
    public boolean equals(Object obj) {
      return ((obj instanceof ConsumerRunnable)
              && (((ConsumerRunnable) obj).getConsumerID().equals(consumerID)))
          || consumerID.equals(obj);
    }
  }

  /** The Builder object for this class */
  static class Builder {
    // TODO: Builder pattern w/ a twist so we can make mandatory fields expressive

    private String bootStrapServers;
    private String groupID;
    private Set<String> topics;
    private int numberOfConsumerRunnables;
    private Duration timeOut;

    private ModifiedTopicExec modifiedTopicExec = null;

    Builder(String bootStrapServers, String groupID) {
      this.bootStrapServers = bootStrapServers;
      this.groupID = groupID;
    }

    Builder setNumberOfThreads(int num) {
      numberOfConsumerRunnables = num;
      return this;
    }

    Builder setModifiedTopicExec(ModifiedTopicExec kafkaListener) {
      modifiedTopicExec = kafkaListener;
      return this;
    }

    Builder setPollingTimeout(Duration duration) {
      this.timeOut = duration;
      return this;
    }

    ModifiedTopicWatcher build() {
      ModifiedTopicWatcher topicConsumer = new ModifiedTopicWatcher(this.groupID);
      topicConsumer.bootStrapServers = this.bootStrapServers;
      topicConsumer.modifiedTopicExec = this.modifiedTopicExec;
      topicConsumer.NUMBER_OF_CONSUMER_THREADS = this.numberOfConsumerRunnables;
      topicConsumer.timeOut = this.timeOut;
      return topicConsumer;
    }
  }
}
