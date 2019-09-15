

import java.io.IOException;
import java.util.*;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.*;

public class TestDataMonitor {
    @Test
    public void addOrDeleteTopics() throws IOException {
        /*DataMonitor dataMonitor = new DataMonitor(null, null, null);
        Assert.assertEquals(Collections.<String>emptySet(),dataMonitor.CHILDREN);


        // Tests that when no CHILDREN is set a call to this method will set the CHILDREN.
        List<String> topicCurrentList= Arrays.asList("topic_1","topic2", "topic3");
        Set<String> topicCurrentSet = new HashSet<>(topicCurrentList);
        Set<String> deletedTopicsSet =  Collections.<String>emptySet();
        Set<String> newTopicsSet =  Collections.<String>emptySet();

        Map<String, Set<String>> hashMap = dataMonitor.addOrDeleteTopics(topicCurrentList);

        Assert.assertThat(topicCurrentSet, is(dataMonitor.CHILDREN));
        Assert.assertThat(deletedTopicsSet, is(hashMap.get("deleted_topics")));
        System.out.println(hashMap.get("new_topics"));
        Assert.assertThat(newTopicsSet, is(hashMap.get("new_topics")));


        // Test that adding and deleted topics work
        deletedTopicsSet = new HashSet<>(topicCurrentList);
        topicCurrentList= Arrays.asList("topic_5","topic7");
        topicCurrentSet = new HashSet<>(topicCurrentList);
        newTopicsSet = new HashSet<>(topicCurrentList);

        hashMap = dataMonitor.addOrDeleteTopics(topicCurrentList);

        Assert.assertThat(deletedTopicsSet, is(hashMap.get("deleted_topics")));
        Assert.assertThat(newTopicsSet, is(hashMap.get("new_topics")));
        Assert.assertThat(topicCurrentSet, is(dataMonitor.CHILDREN));

        // Test that added and deleted topics work where there is some intersection w/ Topic cache
        deletedTopicsSet = new HashSet<>(Arrays.asList("topic7"));
        topicCurrentList= Arrays.asList("topic_5","topic666");
        topicCurrentSet = new HashSet<>(topicCurrentList);
        newTopicsSet = new HashSet<>(Arrays.asList("topic666"));

        hashMap = dataMonitor.addOrDeleteTopics(topicCurrentList);

        Assert.assertThat(deletedTopicsSet, is(hashMap.get("deleted_topics")));
        Assert.assertThat(newTopicsSet, is(hashMap.get("new_topics")));
        Assert.assertThat(topicCurrentSet, is(dataMonitor.CHILDREN));*/
    }

    @BeforeAll
    public void cleanUpKafka(){

        // Delete all consumer groups
            // Check if list of consumer groups is empty.

        // Delete all topics
            // check if list of consumer topics is empty.

        // Add a couple of topics
            // add a couple records to a topic

        // Create a temporary file that the kafka watcher's write to.

        // create functions that write to the file
            // a json array of [{topicName:"",record: "", timestamp: "", type: ""}]

        // Launch watcher
            // verify output on initialization.
    }


    @Test
    public void testKafkaWatcher(){
        // Launch watcher

        // Since this is first time launching verify that the topic we added a couple records to
            // is registered by the modification listener

        // Create topics
            // verify watcher catches new topic

        // Delete a couple topic
            // verify watcher catches deleted topic

        // Add records to existing topics
            // verify watcher catches these additions

    }


}
