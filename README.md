If KafkaTopicWatcher does not work make sure Kafka is running, then try `mvn clean`, then try restarting kafka (or your computer if you don't know how to restart kafka properly). If there are a bunch of errors from Kafka internally restart the application.


# What is KafkaTopicWatcher
KafkaTopicWatcher is a deamon that watches for a Apache Kafka cluster state changes, specifically, it watches
for new topics, deleted topics, and finally existing topics that have been modified with new Kafka data records. 
Each time one of these state changes KafkaTopicWatcher is triggered and it launches a user defined function (which can be whatever you want)
for each of these 3 types of state changes. 

# High Level Architecture

`KafkaWatcher` is the primary containter for the KafkaTopicWatcher application. `NewAndDeletedTopicWatcher` and `ModifiedTopicWatcher` are sub-containers. 

It takes as parameters
- Host:Port address of the ZooKeeper service for Kafka Cluster
- Host:Port address of the BootStrap (kafka server) service for Kafka Cluster
- A function that processes new kafka topics [NewTopicExec](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/NewTopicExec.java)
- A function that processes deleted kafka topics [DeletedTopicExec](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/DeletedTopicExec.java)
- A function that processes modifications to existing kafka topics [ModifiedTopicExec](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/ModifiedTopicExec.java)
- The number of consumer threads to launch for the kafka consumer group that will be  listening to modifications on the topics that KafkaWatcher is monitoring. 
- The polling duration in milliseconds that the topic modification listeners will timeout  before polling a topic for new data

## About `NewAndDeletedTopicWatcher`
The Apache Kafka application uses Zookeeper to allow the distributed Kafka Brokers to communicate with each other, and also to save state information (such as consumer offsets). Zookeeper has a concept called "znodes", in ZooKeeper parlance - and these are similar to files and directories. The znode of interest in this context is `"/brokers/topics"`, the children znode being the individual topics for the active Kafka cluster. Zookeeper has a concept called "watches". Clients can set a watch on a znodes. A watch will be triggered and removed when the znode changes. When a watch is triggered the client receives a packet saying that the znode has changed. In this context we set a watch on the `znode=/brokers/topics` with the Zookeeper method  `ZooKeeper.getChildren()` which sets a watch for the WatchEvent `NodeChildrenChanged` - we set this asynchonously. Once this watch is triggered the completion callback, `AsyncCallback.ChildrenCallback.processResult`,  is called by Zookeeper. In particular this callback has a input, `List<String> children`, and so ZooKeeper feeds in the updated set of topics at `time=t` which is different than the set of topics at `time=t-1`. That's basically it, `NewAndDeletedTopicWatcher` has a parameter `Set<String> topicsCache` that keeps the state of the topics at `time=t-1` and uses the updated children at `time=t-1` to determine the new and deleted topics from the previous iteration.

But wait... if a watch is triggered and removed once a znode changes why doesn't the application stop after one "watch"?
Well to set a watch via `ZooKeeper.getChildren()` (or any other watch setting method) the interface `Watcher` must be implemented and method `Watcher.process(WatchedEvent event)` must be overrided. `NewAndDeletedTopicWatcher` has the dual roal as a `Watcher` and if you look at the [code](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/NewAndDeletedTopicWatcher.java#L150) you'll see that the watch is reset via `NewAndDeletedTopicWatcher.getTopics()` each time `event.getType() == Watcher.Event.EventType.NodeChildrenChanged` happens. Also note that `ZooKeeper.getChildren()` has the *ability* to set a watch its main purpose is return a list of children. So the first call to `ZooKeeper.getChildren()` returns a list of topics (asynchonously) and sets a watch; that's how `Set<String> topicsCache` is initially set. 

 You will notice that in the [code](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/NewAndDeletedTopicWatcher.java#L259) `AsyncCallback.ChildrenCallback.processResult` there is a called to `NewAndDeletedTopicWatcher.Executor.launchExecutable()` which processes the new/deleted topics -- we will talk about this `NewAndDeletedTopicWatcher.Executor` later. 
 

## About `ModifiedTopicWatcher` 

`ModifiedTopicWatcher` provides three static public functions: `shutdown()`, `addTopic()`, `removeTopic()`. These functions are a lot like OS system calls; e.g  they interrupt `ModifiedTopicWatcher.run()`, then `ModifiedTopicWatcher.run()` calls the appropriate  internal functions  -- again  a lot like  OS internal kernell functions. At a high level ModifiedTopicWatcher manages a bunch of kafka consumer threads that all have the same groupID. `ModifiedTopicWatcher` uses `LoadBalancer` to assign and remove topics from the topicSets of its individual consumer threads, and it uses the function `ModifiedTopicWatcher.updateConsumerTopicSet()` to interrupt the kafka topic polling of the consumer thread instance to give to the instance a new topic set to watch. 

Let's explain a couple parameters  of `ModifiedTopicWatcher` and hopefully the architecture makes sense:
- **groupID**: The kafka consumer groupID. The groupID also uniquely identifies a `ModifiedTopicWatcher` (*in case you launch multiple and want to load balance topics between multiple instances of `ModifiedTopicWatcher`, or give specific topics to certain types of `ModifiedTopicWatcher` instances.*)
- **timeOut**: So, `ModifiedTopicWatcher` launches a bunch of consumer runnable threads and these threads contain a kafka consumer that belong to the the consumer group `groupID`. To get data from topic(s) Kafka provides a mechanism `KafkaConsumer.poll(Duration timeout)` which polls the topic(s) every `timeout` milliseconds. Hence, the parameter **timeOut** sets the polling duration. Note, once a KafkaConsumer is polling it can't stop unless someone calls `KafkaConsumer.wakeup()`. `KafkaConsumer.wakeup()` causes a `WakeupException` to be thrown by  `KafkaConsumer.poll()`. Hence the reason `ModifiedTopicWatcher.ConsumerRunnable.pollTopicAndExec()` throws `WakeupException`. 

- **NUMBER_OF_CONSUMER_THREADS** The maximum number of consumer threads an instance of `ModifiedTopicWatcher` can launch.

- **Phase PHASER = new Phaser(1);** What the heck is this??? It's a flexible thread synchonization method for threading. So to understand [Phaser's](https://knpcode.com/java/concurrency/phaser-in-java-concurrency/) you need to understand [CountDownLatches](https://knpcode.com/java/concurrency/countdownlatch-in-java/). To create a CountDownLatch you first have to call `CountDownLatch(int count)`, and then some main threads launches a few worker threads and calls `CountDownLatch.await()` causing the main thread to wait until the latch has counted down to zero. In the canonical thread pool example, after each worker thread has finished their work they call  `CountDownLatch.countDown()` and exit, this call decrements the count. So if you want to manage a set thread pool of size `n` then have each of the `n` workers  call `CountDownLatch.countDown()` upon completion, and hence the count will be zero when the thread pool has finished its work, and once this happens `CountDownLatch.await()` in the main thread will stop waiting. So the problem with this is that the count is fixed and not dynamic. Our use case needs more flexibility, and hence we use a similar synchonization tool called the Phaser.

We only want to launch the amount of consumer threads needed and nothing more so if there are 5 topics, and `NUMBER_OF_CONSUMER_THREADS = 10` then `ModifiedTopicWatcher` will only launch 5 consumer threads to watch each of the 5 topics, and if another topic comes in then `ModifiedTopicWatcher` launches another consumer thread, and if 15 topics come in then `ModifiedTopicWatcher` will launch at most `NUMBER_OF_CONSUMER_THREADS =10` then use the load balancer to assign the remaining topics to the existing consumer threads. The same logic applies if we delete a topic from a consumer threads topicSet, if the topicSet of the aforementioned consumer thread is empty we want to decommission this consumer thread. Since it must be the case that the `total number of topics < NUMBER_OF_CONSUMER_THREADS` *(the `LoadBalancer` ensures that this is the case)*. 
 - `phaser.awaitAdvanceInterruptibly(int phase)` - equiv to `CountDownLatch.await()`
 - `phaser.arriveAndDeregister()` - equiv to `CountDownLatch.countDown()`
 - `phaser.PHASER.register()` - equiv to incrementing up the count in a CountDownLatch dynamically
 -  We initialize with `Phaser PHASER = new Phaser(1);` registering the main thread. 
So let's run through the rough logic of exiting/shutdown that `ModifiedTopicWatcher` uses. First, the main thread calls the shutdown method for each consumer thread running, and then calls `phaser.arriveAndDeregister()`  for each of the consumer threads that were shutdown, and then call `phaser.arriveAndDeregister()` one more time to deregister the main thread, and then sets `this.isDead = true`, and then the main thread will know its time to die. This logic is implemented in   `ModifiedTopicWatcher._shutdown()` and `ModifiedTopicWatcher.run()`  --just trace the method calls. A phaser has more interesting uses than this [see](https://knpcode.com/java/concurrency/phaser-in-java-concurrency/). 

- **mapOfConsumerInstances**  is a map of the current consumer runnables active in `ModifiedTopicWatcher` to a unique identifier called  `consumerID`. `consumerID` semantically means nothing and they are generated each time a new consumer thread is generated via `ModifiedTopicWatcher.generateConsumerID(void)`

## About `LoadBalancer` - round robin style

Is pretty well documented and is based on [StackOverFlow](https://stackoverflow.com/questions/39489193/algorithms-for-rebalancing-round-robin-assignments). There is a slight twist in our implementation since we want to minimize the number of rebalancing -- opting for switching the order of workers instead. See the comments of the first solution to [StackOverFlow](https://stackoverflow.com/questions/39489193/algorithms-for-rebalancing-round-robin-assignments) to see why its not efficient, then look at `LoadBalancer.removeJob()` to see our fix.


## Depedencies: `NewAndDeletedTopicWatcher.Executer`

So there is an inherent interaction between `ModifiedTopicWatcher` and `NewAndDeletedTopicWatcher`. Each time a new topic gets added we want to call `NewAndDeletedTopicWatcher` to process the topic but we *also* want to call `ModifiedTopicWatcher.addTopic(Collection<String> topicSet)` to set a watch on the topic so we can be notified of updates . Each time a topic gets deleted we want to call `NewAndDeletedTopicWatcher` to process the topic but we *also* want to call `ModifiedTopicWatcher.removeTopic(Collection<String> topicSet)`  to stop watching for updates. How do we do this without making things to messy. The interface `NewAndDeletedTopicWatcher.Executer` takes care of this, and `KafkaWatcher` implements the sole method of this interface `@Override public void launchExecutable(Map<String, Set<String>> addedAndDeletedTopics)` where `addedAndDeletedTopics` is a map with two keys "deleted_topics" and "new_topics". `ModifiedTopicWatcher.Executer.launchExecutable()` is called [here](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/NewAndDeletedTopicWatcher.java#L259); notice this is in the Zookeeper completion callback `AsyncCallback.ChildrenCallback.processResult` meaning everytime a topic is added/deleted `ModifiedTopicWatcher.Executer.launchExecutable()` will be called. `ModifiedTopicWatcher.Executer.launchExecutable()` implementation is located [here](https://github.ibm.com/Jide-Anene/kafkaTopicWatcher/blob/master/src/main/java/topicWatchClient/KafkaWatcher.java#L259); notice how we first check if this `newAndDeletedTopicWatcher.topicsCache` has initialized and if it has not aka `newTopics.isEmpty() & deletedTopics.isEmpty()` then we call `ModifiedTopicWatcher.addTopic()` with input `newAndDeletedTopicWatcher.topicsCache`, hence setting a watch on the existing topics. Else, we check if there are deletedTopics or newTopics, and call the appropriate methods if this is the case. 

## About: `KafkaWatcher`

The primary role of `KafkaWatcher` is to manage the interplay between `ModifiedTopicWatcher` and  `NewAndDeletedTopicWatcher`, via implementation of the interface `NewAndDeletedTopicWatcher.Executer`. `KafkaWatcher` first creates instances of `ModifiedTopicWatcher` and `NewAndDeletedTopicWatcher` and then launches a thread that the runs the run method of `NewAndDeletedTopicWatcher`. Via `ModifiedTopicWatcher.Executer.launchExecutable()`, `ModifiedTopicWatcher.addTopic()` and `ModifiedTopicWatcher.removeTopic()` are called when a new topic is discovered or deleted respectively. 

`KafkaWatcher` also implements a shutdown hook so that `NewAndDeletedTopicWatcher` and  `ModifiedTopicWatcher` shutdown correctly upon system exit. 

# For the Future
- Set security/permissions so that a client can not delete a consumer group watching his/her topics
- Look into the "znode=/admin/deleted_topics". Potentially we can set a watch on this znode to get back the deleted topics. 
- Turn logging on and replace `System.print` statements. Beware kafka really throws alot of messages to the log gets distracting when developing.
- Problem: Millions of topics in practice so the `set` structure that we are using is not optimal so at minimum change this to a trie or b+ tree
  - Better Solution: Split `NewAndDeletedTopicWatcher` into two classes `NewTopicWatcher` and `DeletedTopicWatcher`.
   - Have a main process called `Watcher` that calls `NewTopicWatcher` to manage new incoming topics (need a trie or b+tree.) `Watcher` calls `LoadBalancer` to send new topics round robin style over the network to its collection of **distributed workers** that are uniquely identified by `groupID`. 
    - **Distributed Workers** refactor `KafkaWatcher` to be the bundle of `DeletedTopicWatcher` and `ModifiedTopicWatcher`,  `KafkaWatcher` will manage its topicSet watching for modifications to the topics it is watching, and watching for deletions only. It will also recieve new topics from the network via `Watcher`. It will contain a topicSet that can only be updated by the master `Watcher` via network calls. `DeletedTopicWatcher` will watch "znode=/admin/deleted_topics" looking for deletions to its topic set only. `ModifiedTopicWatcher` will launch on new topics delivered by `Watcher`. 
    - Containerize and come up with configuration protocols `Watcher` can send to its **distributed workers**. 
 - Create a custom znode for the `Watcher` that saves state information such as the groupID of its worker `KafkaWatcher` and the topicSets that each of these workers have. Why? When you reboot you want the same worker to watch the same topics since kafka saves consumer offets; this way a worker isnt doing work twice and can continue work from where it left off.  
    
    
SCALE
    
   

