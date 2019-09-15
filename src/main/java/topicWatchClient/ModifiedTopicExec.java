package topicWatchClient;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ModifiedTopicExec {
  /** @param kafkaRecords A collection of kafka topics to process. */
    void onStateChange(ConsumerRecords<String, String> kafkaRecords);
    }
