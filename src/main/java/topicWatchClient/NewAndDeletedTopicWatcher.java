//

package topicWatchClient;

// ToDo: Changes the KafkaCache to some sort of tree structure -- trie
// ToDo: Millions of topics potentially how do we watch all of them -- probably through kubernetes
// ToDo: Security -- don't want people deleteting
// ToDo: Scale -- Make it a trie or B+ tree of existing nodes

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A <u>watch</u> is a one-time trigger associated with a znode and a type of event (e.g., data is
 * set in the znode, or the znode is deleted). When an application process registers a watch to
 * receive a notification, the watch is triggered at most once and upon the first event that matches
 * the condition of the watch.
 */

/**
 * A <u>notification</u> is generated once a watch is triggered by an event. it is a message to the
 * application client that registered the watch to inform this client of the event.
 */

/**
 * The NewAndDeletedTopicWatcher's job is to start and stop the executable passed in. It does this
 * by setting a watch on the znode="/brokers/topics", continuously resetting the watch after
 * the notification=Watcher.Event.EventType.NodeChildrenChanged is received.
 *
 * Based very loosely on <a href="https://stackoverflow.com/questions/36153783/kafka-consumer-to-dynamically-detect-topics-added">
 *     StackOver:kafka-consumer-to-dynamically-detect-topics-added</a>
 */
public class NewAndDeletedTopicWatcher implements Watcher, Runnable {

  //
  // Set by the builder
  //

  // host:port of the zookeeper servers, multiple servers can be in a comma separated
  // string list e.g. "host:port,host:port"
  private String zookeeperHostPort;
  // the lock to access the topicsCache
  private ReentrantReadWriteLock topicsCacheLock;

  //
  // Set internally
  //

  // used to communicate w/ zookeeper API
  private ZooKeeper zooKeeper;
  // isDead=true if `NewAndDeletedTopicWatcher' is dead, otherwise false
  public volatile boolean isDead;
  // The cache of topics currently active
   volatile Set<String> topicsCache;
   // latch used to control concurrency for `NewAndDeletedTopicWatcher'
  CountDownLatch latch = new CountDownLatch(1);

  //
  // Constants
  //
  private static final Logger LOG = LoggerFactory.getLogger(NewAndDeletedTopicWatcher.class);
  private static final String ZNODE = "/brokers/topics";
  private NewAndDeletedTopicWatcher.Executor executor;


  /** The constructor which is private because we have a builder. */
  private NewAndDeletedTopicWatcher(String zookeeperHostPort, Executor executor, ReentrantReadWriteLock topicsCacheLock) {
    this.zookeeperHostPort = zookeeperHostPort;
    this.executor = executor;
    this.topicsCacheLock = topicsCacheLock;

    System.out.println("\t[NewAndDeletedTopicWatcher] NewAndDeletedTopicWatcher initialized");
    try {
      zooKeeper = new ZooKeeper(zookeeperHostPort, 3000, this);
      getTopics();
    } catch (IOException e) {
      LOG.error("\t[NewAndDeletedTopicWatcher] ZooKeeper Received error signal",e);
    }
  }

  /** Call this function to get the ball rolling. */
  // FixMe: Looks cute might delete later
  public void startListening() {
    Thread thread = new Thread(this);
    thread.start();
  }

  /**
   * Overrides Runnable.run() this method is called when the Runnable is executed in a thread
   *
   * <p>Dealing w/ Interrupts: <a href="https://www.yegor256.com/2015/10/20/interrupted-exception.html">Java Interrupts</a>
   * <p>Dealing w/ Sync Locks: <a href="https://docs.oracle.com/javase/tutorial/essential/concurrency/locksync.html">Java Locks</a>
   */
  @Override
  public void run() {
    try {
      latch.await();
    } catch (InterruptedException e){
      if (isDead){
        System.out.println("\t[NewAndDeletedTopicWatcher] Shutting down. ");
        latch.countDown();
        Thread.currentThread().interrupt();
      }else{
        LOG.error("\t[NewAndDeletedTopicWatcher] Erroneous interrupt shutting down... ",e);
        isDead=true;
        latch.countDown();
        Thread.currentThread().interrupt();
      }

    }
  }

  /**
   * Call the async method ZooKeeper.getChildren() set a watch for when the list of Kafka topics
   * that have change; aka created or deleted.
   */
  void getTopics() {
    zooKeeper.getChildren(ZNODE, true, getTopicsCallback, null);
  }

  /**
   * Getter for topicsCache
   * */
  Set<String> getTopicsCache() {
    topicsCacheLock.readLock().lock();
    Set<String> copiedSet = new HashSet<>(topicsCache);
    topicsCacheLock.readLock().unlock();

    return copiedSet;
  }

  /**
   * Removes a topic from the topicCache
   *
   * @param topicName the name of topic to be removed
   */
  synchronized void removeTopicFromCache(String topicName) {
    topicsCacheLock.writeLock().lock();
    try {
      topicsCache.remove(topicName);
    } finally {
      topicsCacheLock.writeLock().unlock();
    }
  }

  /**
   * Processes the watch event `NodeChildrenChanged' -- meaning new topics have been added or deleted
   * from the znode `/brokers/topics' -- also does bit of error handling.
   *
   * @param event a watched event which is either: NodeChildrenChanged, NodeDataChanges,
   *     NodeCreated, NodeCreated.
   */
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      assert ZNODE.equals(event.getPath());
      getTopics();
    }

    if (event.getType() == Watcher.Event.EventType.None) {
      // We are are being told that the state of the connection has changed
      switch (event.getState()) {
        case SyncConnected:
          // In this particular example we don't need to do anything
          // here - watches are automatically re-registered with
          // server and any watches triggered while the client was
          // disconnected will be delivered (in order of course)
          break;
        case Expired:
          closing(KeeperException.Code.SESSIONEXPIRED.intValue());
          isDead=true;
          break;
      }
    }
  }

  /**
   * The executor job is to be a class called by the parent container `KafkaWatcher' and it
   * deals with the inherent dependencies between `ModifiedTopicWatcher' and `NewAndDeletedTopicWatcher'.
   * So all the dependencies between those two classes will be resolved in {@link Executor#launchExecutable(Map)}
   */
  public interface Executor {
    /**
     * Runs the `deletedTopicExec' on topics that have been deleted, and `newTopicExec' on topics
     * that have been created.
     */
    void launchExecutable(Map<String, Set<String>> addedAndDeletedTopics);
  }


  private void closing(int rc) {
    // decrement countdown latch
    latch.countDown();
    isDead = true;
  }

  /**
   * kills the `NewAndDeletedTopicWatcher'
   * */
  public static void shutdown(NewAndDeletedTopicWatcher newAndDeletedTopicWatcher,
                              Thread newAndDeletedTopicWatcherThread){

    newAndDeletedTopicWatcher.isDead = true;
    newAndDeletedTopicWatcherThread.interrupt();
  }


  /**
   * Adds children (or in this context `topics`) to existing list of children and returns the
   * children that were deleted or added from the last iteration. Also updates the `topicsCache`. 
   *
   * @param topicNames list of topics from current iteration
   */
  @VisibleForTesting
  public Map<String, Set<String>> addOrDeleteTopics(List<String> topicNames) {

    // Convert to list of topicsNames to set of topicNames - O(n)
    Set<String> currentTopics =
            topicNames.stream()
                    .filter((topicName) -> (!topicName.equals("__consumer_offsets")))
                    .collect(Collectors.toSet());

    Map<String, Set<String>> hashMap = new HashMap<>();

    topicsCacheLock.writeLock().lock();
    try {
      if (topicsCache == null) {
        topicsCache = currentTopics;

        hashMap.put("new_topics", Collections.<String>emptySet());
        hashMap.put("deleted_topics", Collections.<String>emptySet());
      }
      // else  we need find the added + deleted topics from the last iteration
      else {
        // Set difference - O(n)
        Set<String> topicsDeleted = Sets.difference(topicsCache, currentTopics);
        Set<String> topicsAdded = Sets.difference(currentTopics, topicsCache);

        topicsCache = currentTopics;

        hashMap.put("new_topics", topicsAdded);
        hashMap.put("deleted_topics", topicsDeleted);

      }
    } finally {
      topicsCacheLock.writeLock().unlock();
    }
    return hashMap;
  }

  /** The Zookeeper completion callback for the function {@link #getTopics()} */
  private AsyncCallback.ChildrenCallback getTopicsCallback =
      new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
          switch (KeeperException.Code.get(rc)) {
            case NOAUTH:
            case NONODE:
            case SESSIONEXPIRED:
              // unrecoverable error
              LOG.error(
                  "\t[NewAndDeletedTopicWatcher#getTopicsCallback()] Unrecoverable error",
                  KeeperException.create(KeeperException.Code.get(rc), path));
                  closing(rc);
                  isDead=true;
              break;
            case OK:
              System.out.println(
                  "\t[NewAndDeletedTopicWatcher#getTopicsCallback()] Successfully got a list of "
                      + children.size()
                      + " topics: ");
              executor.launchExecutable(addOrDeleteTopics(children));
              break;
            default:
              // retry errors
              getTopics();
          }
        }
      };

  
  public static class Builder {
    private String zookeeperHostPort;
    private NewAndDeletedTopicWatcher.Executor executor;
    private ReentrantReadWriteLock topicsCacheLock;

    public Builder(
        String zooKeeperHostPort, ReentrantReadWriteLock topicsCacheLock, Executor executor) {
      this.topicsCacheLock = topicsCacheLock;
      this.executor = executor;
      this.zookeeperHostPort = zooKeeperHostPort;
    }

    public NewAndDeletedTopicWatcher build() {

      return new NewAndDeletedTopicWatcher(this.zookeeperHostPort,
              this.executor,
              this.topicsCacheLock);
    }
  }
}
