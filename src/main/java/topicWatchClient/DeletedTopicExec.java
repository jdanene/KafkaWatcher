package topicWatchClient;


import java.util.Collection;

public interface DeletedTopicExec {
    /**
     *
     * @param kafkaTopic The name of a kafka topics to process.
     * */
     void onStateChange(Collection<String> kafkaTopic);
}
