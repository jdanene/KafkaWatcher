package topicWatchClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <center><b>Program Design</b></center> <br>
 * The KafkaWatcher application is broken into 3 units:
 *
 * <ul>
 *   <li>{@link topicWatchClient.ModifiedTopicWatcher}, which watches for modifications to existing
 *       topics
 *   <li>the {@link topicWatchClient.NewAndDeletedTopicWatcher} which watches the kafka cluster for
 *       new and deleted topics
 *   <li>and the {@link topicWatchClient.KafkaWatcher} which manages the dependence * between {@link
 *       topicWatchClient.ModifiedTopicWatcher} & {@link topicWatchClient.NewAndDeletedTopicWatcher}
 *       and launches the KafkaWatcher application
 * </ul>
 *
 * The KafkaWatcher class is the primary container for the KafkaWatcher application<br>
 * It takes as parameters:
 *
 * <ol>
 *   <li>Host:Port address of the ZooKeeper service for Kafka Cluster
 *   <li>Host:Port address of the BootStrap (kafka server) service for Kafka Cluster
 *   <li>A function that processes new kafka topics {@link NewTopicExec}
 *   <li>A function that processes deleted kafka topics {@link DeletedTopicExec}
 *   <li>A function that processes modifications to existing kafka topics {@link ModifiedTopicExec}
 *   <li>The number of consumer threads to launch for the kafka consumer group that will be
 *       listening for modifications the topics the KafkaWatcher is watching
 *   <li>The polling duration in milliseconds that the topic modification listeners will timeout
 *       before polling a topic for new data
 * </ol>
 *
 */
public class KafkaWatcher implements NewAndDeletedTopicWatcher.Executor, Runnable {

  public static void main(String[] args) {
    // The zookeeper server and kafka server host:ports
    String zooKeeperHostPort = "127.0.0.1:2181";
    String bootStrapServers = "127.0.0.1:9092";

    // The unique identifier for the KafkaWatcher instance.
    String groupID = setGroupID("group1");

    final String ANSI_RESET = "\u001B[0m";
    final String ANSI_PURPLE = "\u001B[35m";

    // Create the executables that will process the information
    NewTopicExec newTopicExec =
        (topicNames) ->
            topicNames.forEach((topicName) -> System.out.println(ANSI_PURPLE+"**(Passed in fun call)** New Topic: " + topicName+ANSI_RESET));

    DeletedTopicExec deletedTopicExec =
        (topicNames) ->
            topicNames.forEach((topicName) -> System.out.println(ANSI_PURPLE+"**(Passed in fun call)** Deleted: " + topicName+ANSI_RESET));

    ModifiedTopicExec modifiedTopicExec =
        (kafkaRecords) ->
            kafkaRecords.forEach(
                (record) -> {
                  System.out.println(ANSI_PURPLE+"**(Passed in fun call)** Topic: " + record.topic()+ANSI_RESET);
                  System.out.println(ANSI_PURPLE+"**(Passed in fun call)** Key: " + record.key() + ", Value: " + record.value()+ANSI_RESET);
                  System.out.println(
                          ANSI_PURPLE+"**(Passed in fun call)** Partition: " + record.partition() + ", Offset:" + record.offset()+ANSI_RESET);
                });

    // Create the Kafka Watcher Object
    KafkaWatcher kafkaWatcher =
        new KafkaWatcher.Builder(zooKeeperHostPort, bootStrapServers, groupID)
            .setDeletedTopicExec(deletedTopicExec)
            .setModifiedTopicExec(modifiedTopicExec)
            .setNewTopicExec(newTopicExec)
            .build();

    // Run the watcher synchronously
    kafkaWatcher.run();
  }


  private KafkaWatcher() {}

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWatcher.class);

  // The topicsCache which holds the currently active topics and the lock for the cache
  private ReentrantReadWriteLock topicsCacheLock = new ReentrantReadWriteLock();

  // This is the watcher object for new and deleted topics.
  private NewAndDeletedTopicWatcher newAndDeletedTopicWatcher;

  // This is the watcher object for modifications to existing topics
  private ModifiedTopicWatcher modifiedTopicWatcher;

  // The size of the thread pool managed by `modifiedTopicWatcher`: Default is 8 but this can be set
  // via Builder
  private Integer modifiedTopicWatcherThreadPoolSize = 8;

  // The the timeout duration for consumer polling w/in `modifiedTopicWatcher` default is 100
  // milliseconds
  private Duration modifiedTopicWatcherTimeOut;

  /* ******************************************************************************************************
   * A groupID (String)  uniquely maps to a `ModifiedTopicWatcher` and also map to the kafka Consumer group
   * that the `ModifiedTopicWatcher` is managing. In the future there will be a master Watcher that launches
   * multiple KafkaWatcher and these objects will also be identified by groupID
   * ********************************************************************************************************/
  private String groupID;

  /*
   * *******************************************************************************************************
   * We append the string `CONSUMER_ID_PREFIX' to the front and `CONSUMER_ID_SUFFIX` to the back of the topic name
   * to prevent naming conflicts with end users, since kafka topic consumer groupID need to be globally
   * unique across a kafka cluster. In the future `CONSUMER_ID_PREFIX` and `CONSUMER_ID_SUFFIX` will be moved
   * to the master watcher and KafkaWatcher will not have to worry about this.
   * ****************************************************************************************************
   */
  private static final String CONSUMER_ID_PREFIX = "____";
  private static final String CONSUMER_ID_SUFFIX = "____";

  // The AdminClient can delete topics, consumers, etc.
  @Deprecated private KafkaUtils kafkaAdmin=new KafkaUtils();

  // host:port of the zookeeper servers, multiple servers can be in a comma separated
  // string list e.g. "host:port,host:port"
  private String zooKeeperHostPort;
  // host:port of the bootstrap servers (kafka servers)
  private String bootStrapServers;

  // Executable called when an existing topic receives additional data records
  private ModifiedTopicExec modifiedTopicExec;
  // Executable called when a topic is deleted
  private DeletedTopicExec deletedTopicExec;
  // Executable called when a topic is created
  private NewTopicExec newTopicExec;


  private Thread modifiedTopicWatcherThread;
  private Thread newAndDeletedTopicWatcherThread;


  /** Call this to run the Kafka Watcher. */
  @Override
  public void run() {
    System.out.println("[KafkaWatcher] Starting");

    // create a NewAndDeletedTopicWatcher Object & start it.
    newAndDeletedTopicWatcher =
        new NewAndDeletedTopicWatcher.Builder(zooKeeperHostPort, topicsCacheLock, this).build();
    newAndDeletedTopicWatcherThread = new Thread(newAndDeletedTopicWatcher);
    newAndDeletedTopicWatcherThread.start();

    // create a ModifiedTopicWatcher
    modifiedTopicWatcher =
        new ModifiedTopicWatcher.Builder(bootStrapServers, groupID)
            .setNumberOfThreads(modifiedTopicWatcherThreadPoolSize)
            .setModifiedTopicExec(modifiedTopicExec)
            .setPollingTimeout(modifiedTopicWatcherTimeOut)
            .build();
    modifiedTopicWatcherThread = new Thread(modifiedTopicWatcher);
    modifiedTopicWatcherThread.start();

    // add shutdown hook so application can properly close on system.exit()
    shutdownHook();

    /*
     * See --> https://stackoverflow.com/questions/13249835/java-does-wait-release-lock-from-synchronized-block
     * */

    synchronized (this) {
      while (!newAndDeletedTopicWatcher.isDead) {
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.error("[KafkaWatcher] interrupted by", e);
        }
      }
    }

  }

  /** Call this function to stop the kafkaWatcher from running. */
  public void close() {
    synchronized (this) {
      shutdown();
      notifyAll();
    }
  }

  /**
   * Returns a properly formatted group name by appending 'CONSUMER_ID_PREFIX' and
   * 'CONSUMER_ID_SUFFIX' to input
   *
   * @param name the name pre-format
   * @return a properly formatted group name
   */
  private static String setGroupID(String name) {
    return CONSUMER_ID_PREFIX + name + CONSUMER_ID_SUFFIX;
  }

  /** Shutdown the newAndDeletedTopicWatcher and all of the active newAndDeletedTopicWatcher's */
  private void shutdown() {
    // close the newAndDeletedTopicWatcher thread.
    NewAndDeletedTopicWatcher.shutdown(newAndDeletedTopicWatcher,newAndDeletedTopicWatcherThread);

    // Shut down modifiedTopicWatcherThread
    ModifiedTopicWatcher.shutdown(modifiedTopicWatcherThread);
  }

  /**
   * A shutdown hook is a construct that allows us to plug in a piece of code to be executed when
   * the JVM is shutting down. Useful when need to do special clean up operations in case the VM is
   * shutting down. Handling this using the general constructs such as making sure that we call a
   * special procedure before the application exits -- e.g. System.exit(0) -- will not work for
   * situations where the VM is shutting down due to an external reason -- e.g. kill request from
   * O/S -- or due to a resource problem -- e.g out of memory.
   */
  private void shutdownHook() {
    Thread mainThread = Thread.currentThread();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("[KafkaWatcher] Caught shutdown hook");
                  close();
                  try {
                    mainThread.join(); // waits for the thread to terminate
                  } catch (InterruptedException e) {
                    LOG.error("[KafkaWatcher] interrupted during shutdown hook", e);
                  }
                }));
  }


  /**
   * If `newTopics` and `deletedTopics` are empty then it must be the case that kafkaWatcher just
   * initialized
   */
  private boolean hasInitialized(Collection<String> newTopics, Collection<String> deletedTopics) {
    return !(newTopics.isEmpty() & deletedTopics.isEmpty());
  }

  /**
   * Runs the `deletedTopicExec' method on topics that have been deleted, and `newTopicExec' method
   * on topics that have been created, and then adds the topic to `ModifiedTopicWatcher` to be
   * watched
   *
   * @param addedAndDeletedTopics A map of new and deleted topics: {"deleted_topics": Set<String>,
   *     "new_topics": Set<String>}
   */
  @Override
  public void launchExecutable(Map<String, Set<String>> addedAndDeletedTopics) {
    System.out.println("\t[NewAndDeletedTopicWatcher.Executor] Invoking launchExecutable()");
    Collection<String> deletedTopics = addedAndDeletedTopics.get("deleted_topics");
    Collection<String> newTopics = addedAndDeletedTopics.get("new_topics");

    if (!(hasInitialized(newTopics, deletedTopics))) {
      System.out.println("\t[NewAndDeletedTopicWatcher.Executor] Initializing for existing topics:"+newAndDeletedTopicWatcher.topicsCache);

      topicsCacheLock.readLock().lock();
      try {
        ModifiedTopicWatcher.addTopic(newAndDeletedTopicWatcher.topicsCache,
                modifiedTopicWatcher,
                modifiedTopicWatcherThread);
      } catch (Exception e){
        e.printStackTrace();
      }
      finally {
        topicsCacheLock.readLock().unlock();
      }
      System.out.println("\t[NewAndDeletedTopicWatcher.Executor] Finished Initializing");

    } else {

      if (!(deletedTopics.isEmpty())) {
        // ToDo: Make this a thread queue - or manage this some how to perform asynch
        System.out.println("\t[NewAndDeletedTopicWatcher.Executor] Deleting topics: "+deletedTopics);
        deletedTopicExec.onStateChange(deletedTopics);

        ModifiedTopicWatcher.removeTopic(deletedTopics,
                modifiedTopicWatcher,
                modifiedTopicWatcherThread);
      }

      if (!(newTopics.isEmpty())) {
        // ToDo: Make this a thread queue - or manage this some how to perform asynch
        System.out.println("\t[NewAndDeletedTopicWatcher.Executor] Adding topics: "+deletedTopics);

        newTopicExec.onStateChange(newTopics);

        ModifiedTopicWatcher.addTopic(newTopics,
                modifiedTopicWatcher,
                modifiedTopicWatcherThread);
      }
    }
  }

  // TODO: Builder pattern w/ a twist make mandatory fields expressive
  public static class Builder {

    private String zookeeperHostPort;
    private String bootStrapServers;
    private ModifiedTopicExec modifiedTopicExec;
    private DeletedTopicExec deletedTopicExec;
    private NewTopicExec newTopicExec;
    private String groupID;
    private Duration modifiedTopicWatcherTimeOut = Duration.ofMillis(100);
    private Integer modifiedTopicWatcherThreadPoolSize = 8;

    public Builder(String zooKeeperHostPort, String bootStrapServers, String groupID) {
      this.zookeeperHostPort = zooKeeperHostPort;
      this.bootStrapServers = bootStrapServers;
      this.groupID = groupID;
    }

    public Builder setModifiedTopicExec(ModifiedTopicExec exec) {
      this.modifiedTopicExec = exec;
      return this;
    }

    public Builder setDeletedTopicExec(DeletedTopicExec exec) {
      this.deletedTopicExec = exec;
      return this;
    }

    public Builder setNewTopicExec(NewTopicExec exec) {
      this.newTopicExec = exec;
      return this;
    }

    public Builder setPollingTimeout(Duration timeout) {
      this.modifiedTopicWatcherTimeOut = timeout;
      return this;
    }

    public Builder setThreadPoolSize(Integer numOfThreads) {
      this.modifiedTopicWatcherThreadPoolSize = numOfThreads;
      return this;
    }

    public KafkaWatcher build() {
      KafkaWatcher kafkaWatcher = new KafkaWatcher();
      kafkaWatcher.zooKeeperHostPort = this.zookeeperHostPort;
      kafkaWatcher.bootStrapServers = this.bootStrapServers;
      kafkaWatcher.deletedTopicExec = this.deletedTopicExec;
      kafkaWatcher.modifiedTopicExec = this.modifiedTopicExec;
      kafkaWatcher.newTopicExec = this.newTopicExec;
      kafkaWatcher.groupID = this.groupID;
      kafkaWatcher.modifiedTopicWatcherTimeOut = this.modifiedTopicWatcherTimeOut;
      kafkaWatcher.modifiedTopicWatcherThreadPoolSize = this.modifiedTopicWatcherThreadPoolSize;
      return kafkaWatcher;
    }
  }
}
