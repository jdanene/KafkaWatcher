import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topicWatchClient.DeletedTopicExec;
import topicWatchClient.ModifiedTopicExec;
import topicWatchClient.NewTopicExec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

/**
 * Sample {@link topicWatchClient.NewTopicExec}, {@link topicWatchClient.ModifiedTopicExec}, and
 * {@link topicWatchClient.DeletedTopicExec} implementations.
 */
public class SampleKafkaExec {

  public NewTopicExec newTopicExec;
  public ModifiedTopicExec modifiedTopicExec;
  public DeletedTopicExec deletedTopicExec;

  SampleKafkaExec(String rootFilePath) throws FileNotFoundException {
    this.newTopicExec = new TopicExecutor("newTopicExec", rootFilePath);
    this.modifiedTopicExec = new TopicExecutor("modifiedTopicExec", rootFilePath);
    this.deletedTopicExec = new TopicExecutor("deletedTopicExec", rootFilePath);
  }

  public class TopicExecutor implements NewTopicExec, ModifiedTopicExec, DeletedTopicExec {
    // the executor method type is either "newTopicExec", "modifiedTopicExec", "deletedTopicExec"
    String methodType;
    // boolean used in appendJsonToJsonArrayFile()
    boolean isFileEmpty = true;
    // the path to the file that the TopicExecutor is `writing` to
    String absoluteFilePath;

    /**
     * Creates the `TopicExecutor`
     * @param methodType either "newTopicExec", "modifiedTopicExec", "deletedTopicExec"
     */
    TopicExecutor(String methodType, String rootFilePath) throws FileNotFoundException {
      this.methodType = methodType;
      this.absoluteFilePath = Paths.get(rootFilePath.toString(), methodType+".json").toString();
    }

    private void writeToFile(RandomAccessFile file, String jsonStringObj) throws IOException {
      System.out.println(jsonStringObj);

      if (isFileEmpty) {
        file.writeBytes("[" + jsonStringObj + "]");
        isFileEmpty = false;
      } else {
        // set the file cursor to the position of the char "]"
        long pos = file.length();
        while (file.length() > 0) {
          pos--;
          file.seek(pos);
          if (file.readByte() == ']') {
            file.seek(pos);
            break;
          }
        }
        // write a comma (if is not the first element), the new json element and the char "]"
        file.writeBytes("," + jsonStringObj + "]");
      }
    }

    /**
     * Converts kafkaTopic name to a jsonObject and appends it to end of json array that is stored
     * in a file
     *
     * @param kafkaTopic a kafka topic name
     */
    private void appendJsonToJsonArrayFile(Collection<String> kafkaTopic) throws IOException {
      RandomAccessFile file = new RandomAccessFile(this.absoluteFilePath, "rw");

      for (String topic : kafkaTopic) {
        String jsonStringObj =
                String.format(
                        "{\"exec_type\": \"%s\", \"topic_name\": \"%s\", \"record\": %s }",
                        methodType, topic, "null");
        writeToFile(file, jsonStringObj);
      }
      file.close();
    }

    /**
     * Convert each record in `kafkaRecords` to a jsonObject and appends it to end of json array
     * that is stored in a file
     *
     * @param kafkaRecords A collection of kafka consumer records.
     */
    private void appendJsonToJsonArrayFile(ConsumerRecords<String, String> kafkaRecords)
        throws IOException {

      RandomAccessFile file = new RandomAccessFile(this.absoluteFilePath, "rw");

      for (ConsumerRecord record : kafkaRecords) {
        String jsonRecord =
            String.format("{\"key\": \"%s\", \"value\": \"%s\"}", record.key(), record.value());

        String jsonStringObj =
            String.format(
                "{\"exec_type\": \"%s\", \"topic_name\": \"%s\", \"record\": %s }",
                methodType, record.topic(), jsonRecord);
        writeToFile(file, jsonStringObj);
      }
      file.close();
    }

    /** Write json formatted object to a file, and prints whatever was written */
    @Override
    public void onStateChange(Collection<String> kafkaTopic){
      try{
        appendJsonToJsonArrayFile(kafkaTopic);
      }catch (Exception e){
        e.printStackTrace();
      }    }

    /** Write json formatted object to a file, and prints whatever was written */
    @Override
    public void onStateChange(ConsumerRecords<String, String> kafkaRecords) {
      try{
        appendJsonToJsonArrayFile(kafkaRecords);
      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }
}
