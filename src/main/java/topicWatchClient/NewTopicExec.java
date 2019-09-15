package topicWatchClient;

import java.util.Collection;

public interface NewTopicExec {
    /**
     *
     * @param kafkaTopic The name of a kafka topics to process.
     * */
    void onStateChange(Collection<String> kafkaTopic);
}
