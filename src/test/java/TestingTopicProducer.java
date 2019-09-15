import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Properties;

/** Produces data to a topic */
public class TestingTopicProducer {
  static final String bootStrapServers = "127.0.0.1:9092";

  static void produceData(String topicName, Collection<String> dataRecords) {

      // set producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (String data : dataRecords) {

      // create a producer record
      ProducerRecord<String, String> record = new ProducerRecord<>(topicName, data);

      // send data - asynchronous in background
      producer.send(
          record,
          new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time a record is successfully sent or an exception is thrown
              if (e == null) {
                // record success sent
                System.out.println(
                    "\nReceived new metadata... \n"
                        + "Topic: "
                        + recordMetadata.topic()
                        + "\n"
                        + "Value: "
                        + record.value()
                        + "\n"
                        + "Timestamp: "
                        + recordMetadata.timestamp());
              } else {
                e.printStackTrace();
              }
            }
          });
    }

    // flush data
    producer.flush();
    // flush and close producer
    producer.close();
  }
}
