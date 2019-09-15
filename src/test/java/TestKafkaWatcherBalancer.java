import org.junit.jupiter.api.Test;
import topicWatchClient.LoadBalancer;

import java.util.Arrays;
import java.util.List;

public class TestKafkaWatcherBalancer {
  List<String> CONSUMER_IDs = Arrays.asList("consumer1", "consumer2", "consumer3", "consumer4");

  @Test
  void testAddTopic() {
    // create a load ba$lancer
    List<String> topicNamesList =
        Arrays.asList(
            "topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8",
            "topic9");

    LoadBalancer kafKaWatcherBalancer = new LoadBalancer(CONSUMER_IDs);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);
    kafKaWatcherBalancer.addJob(topicNamesList);
    System.out.println("\n" + kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Remove topic7");
    kafKaWatcherBalancer.removeJob("topic7" );
    System.out.println(kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Remove topic6");
    kafKaWatcherBalancer.removeJob("topic6" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Remove topic9");
    kafKaWatcherBalancer.removeJob("topic9" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Add topic10");
    kafKaWatcherBalancer.addJob("topic10" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Add topic11");
    kafKaWatcherBalancer.addJob("topic11" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n Remove topic1");
    kafKaWatcherBalancer.removeJob("topic1" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n add topic1");
    kafKaWatcherBalancer.addJob("topic1" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);


    System.out.println("\n add worker: consumer5");
    kafKaWatcherBalancer.addWorker("consumer5" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n add topic99");
    kafKaWatcherBalancer.addJob("topic99" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n add topic998");
    kafKaWatcherBalancer.addJob("topic998" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n add topic9968");
    kafKaWatcherBalancer.addJob("topic998" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n remove worker: consumer2");
    kafKaWatcherBalancer.removeWorker("consumer2" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

    System.out.println("\n remove worker: consumer5");
    kafKaWatcherBalancer.removeWorker("consumer5" );
    System.out.println( kafKaWatcherBalancer);
    System.out.println(kafKaWatcherBalancer.previousWorkerIdx);

  }
}
