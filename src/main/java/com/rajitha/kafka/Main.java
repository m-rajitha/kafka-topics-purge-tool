package com.rajitha.kafka;

import com.rajitha.kafka.service.TopicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final TopicService topicService;

    public Main(TopicService topicService) {
        this.topicService = topicService;
    }

    public void run(String[] args) {
        if (args.length < 3) {
            System.err.println("Insufficient number of arguments provided.");
            System.err.println("Usage: java -jar your-jar-file.jar <cluster-name> <cluster-env> <topic-list>");
            System.exit(1);
        }
        // Reading the args
        String clusterName = args[0];
        String clusterEnv = args[1];
        String topics = args[2];

        // Split the topics
        String[] topicArray = topics.split(","); // Modify the delimiter as per your needs
        // Step 2: Convert the array to a List<String> using Arrays.asList()
        List<String> topicList = Arrays.asList(topicArray);

        topicService.clearTopics(clusterName, clusterEnv, topicList);
    }

    public static void main(String[] args) {
        Main main = new Main(new TopicService());
        main.run(args);
        LOGGER.info("Application finished successfully.");
    }
}
