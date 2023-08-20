package com.rajitha.kafka.service;

import com.rajitha.kafka.kafka.AdminClientFactory;
import com.rajitha.kafka.config.kafkaConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicService.class);
    private final AdminClientFactory adminClientFactory;
    private final String asterisks = repeat("*", 42);
    public TopicService() {
        this.adminClientFactory = new AdminClientFactory();
    }

    public void clearTopics(String clusterName,String clusterEnv, List<String> topicList) {
        //Checking the topicList passed is empty
        if (topicList == null || topicList.isEmpty()) {
            throw new IllegalArgumentException("Topic list cannot be null or empty.");
        }
        //Prints the list of topic submitted for purging for user confirmation
        LOGGER.info("Cluster Name: {}", clusterName);
        LOGGER.info("Cluster Environment: {}", clusterEnv);
        LOGGER.info(asterisks);
        LOGGER.info("List of topics submitted for purging ");
        LOGGER.info(asterisks);
        for (String topic : topicList) {
            LOGGER.info(topic);
        }
        LOGGER.info(asterisks);

        //Creating Kafka admin client
        AdminClient adminClient = adminClientFactory.getAdminClient(clusterName, clusterEnv);
        //Creating List to capture status of topic purging for each topic
        List<Map.Entry<String, String>> topicCleanupOut = new ArrayList<>(topicList.size());
        //Looping through topic list to execute clearTopic
        for (String topic : topicList) {
            try {
                boolean result = clearTopic(adminClient, topic);
                if (result) {
                    topicCleanupOut.add(new AbstractMap.SimpleEntry<>(topic, "Success"));
                    LOGGER.info("Successfully cleared topic: " + topic);
                } else {
                    LOGGER.error("Failed to clear topic: " + topic);
                    topicCleanupOut.add(new AbstractMap.SimpleEntry<>(topic, "Failed"));
                }
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.error("An error occurred while clearing topic: " + topic+" due to error : "+e.getLocalizedMessage());
                topicCleanupOut.add(new AbstractMap.SimpleEntry<>(topic, "Failed"));
            }
        }

        // Maximum width for each column
        int maxTopicWidth = 50;
        int maxStatusWidth = 50;

        // Print the table header
        LOGGER.info("+{}+{}+", repeat("-", maxTopicWidth + 2), repeat("-", maxStatusWidth + 2));
        LOGGER.info("| {} | {} |", padToWidth("Topic Name", maxTopicWidth), padToWidth("Cleanup Status", maxStatusWidth));
        LOGGER.info("+{}+{}+", repeat("-", maxTopicWidth + 2), repeat("-", maxStatusWidth + 2));

        // Print each key-value pair in the list as a row in the table
        for (Map.Entry<String, String> entry : topicCleanupOut) {
            LOGGER.info("| {} | {} |", padToWidth(entry.getKey(), maxTopicWidth), padToWidth(entry.getValue(), maxStatusWidth));
        }

        // Print the table footer
        LOGGER.info("+{}+{}+", repeat("-", maxTopicWidth + 2), repeat("-", maxStatusWidth + 2));

        adminClient.close();
    }

    public boolean clearTopic(AdminClient adminClient,String topicName) throws ExecutionException, InterruptedException {
        Boolean returnResult = null;

        LOGGER.info("Topic " + topicName+ " submitted for topic purging");

        Pair<List<TopicPartition>, Map<String, String>> describeTopicResult = describeTopic(adminClient,topicName);
        List<TopicPartition> topicPartitionList = describeTopicResult.getLeft();
        Map<String,String> topicCleanupPolicy = describeTopicResult.getRight();

        LOGGER.info("Got the topic partition , cleanup config details successfully for topic " + topicName);

        if ( StringUtils.equalsIgnoreCase(topicCleanupPolicy.get(kafkaConstants.CLEANUP_POLICY_CONFIG),kafkaConstants.DELETE)){
            for (TopicPartition tp : topicPartitionList) {
                LOGGER.info("Topic: " + tp.topic() + " Partition: " + tp.partition());
            }

            Map<TopicPartition,ListOffsetsResult.ListOffsetsResultInfo> logEndOffsetForTopic = getOffsetsForTopics(adminClient,topicPartitionList,kafkaConstants.LATEST);
            Map<TopicPartition, RecordsToDelete> deleteOffsetMap = new HashMap<>();

            for (TopicPartition topicPartition: topicPartitionList){
                long offset = logEndOffsetForTopic.get(topicPartition).offset();
                LOGGER.info("Latest offset for TopicPartition: " + topicPartition + ", Offset: " + offset);
                deleteOffsetMap.put(topicPartition,RecordsToDelete.beforeOffset(offset));
            }

            DeleteRecordsResult deleteResult = adminClient.deleteRecords(deleteOffsetMap);


            try{
                if (deleteResult.all().get() == null) {
//                    LOGGER.info("Successfully deleted the records for the topic " + topicName);
                    returnResult=true;
                }
            } catch (Exception e) {
                LOGGER.error("An error occurred: ", e);
                returnResult=false;
            }
        }else{
            LOGGER.error("Topic "+kafkaConstants.CLEANUP_POLICY_CONFIG+ " is "+topicCleanupPolicy.get(kafkaConstants.CLEANUP_POLICY_CONFIG) +". Data purging is not available for compact topic for now !!!!!!");
            returnResult=false;
        }
        return returnResult;
    }

    public Pair<List<TopicPartition>, Map<String,String>> describeTopic(AdminClient adminClient,String topicName) throws ExecutionException, InterruptedException {
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        //Get topic partition and topic configuration from cluster
        DescribeTopicsResult describeTopics = adminClient.describeTopics(Collections.singleton(topicName));
        DescribeConfigsResult configsResult = adminClient.describeConfigs(Collections.singleton(topicResource));
        //Getting the partition information
        TopicDescription topicDescription = describeTopics.topicNameValues().get(topicName).get();
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        for(TopicPartitionInfo topicPartitionInfo: topicDescription.partitions()){
            topicPartitionList.add(new TopicPartition(topicName, topicPartitionInfo.partition()));
        }
        //Getting the cleanup policy information
        ConfigEntry topicConfig = configsResult.all().get().get(topicResource).get(TopicConfig.CLEANUP_POLICY_CONFIG);
        Map<String, String> cleanUpConfig = new HashMap<>();
        cleanUpConfig.put(topicConfig.name(),topicConfig.value());

        return  Pair.of(topicPartitionList,cleanUpConfig) ;
    }

    public Map<TopicPartition,ListOffsetsResult.ListOffsetsResultInfo> getOffsetsForTopics(AdminClient adminClient,List<TopicPartition> topicPartitionList, String requestTo) throws InterruptedException, ExecutionException {
        // Get Offset for the topics partitions
        Map<TopicPartition, OffsetSpec> requestOffsets = new HashMap<>();
        for (TopicPartition tp : topicPartitionList) {
            switch (requestTo.toLowerCase()){
                case kafkaConstants.LATEST:
                    requestOffsets.put(tp, OffsetSpec.latest());
                    break;
                case kafkaConstants.EARLIEST:
                    requestOffsets.put(tp, OffsetSpec.earliest());
                    break;
                default:
                    LOGGER.error("Invalid requestTo value: {}", requestTo);
                    return null;
            }
        }
        return adminClient.listOffsets(requestOffsets).all().get();
    }

    private static String padToWidth(String s, int width) {
        if (s.length() > width) {
            s = s.substring(0, width - 3) + "...";
        }
        return String.format("%-" + width + "s", s);
    }

    private static String repeat(String str, int times) {
        return String.join("", Collections.nCopies(times, str));
    }

}
