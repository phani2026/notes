package com.phani.demo.notifications;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

public class KafkaTopicCreation {

    public static void main(String[] args) throws Exception {

        String topicName = "notification-manager";
        KafkaTopicCreation topicCreation = new KafkaTopicCreation();
        topicCreation.createTopic(topicName,3);
    }

    public void createTopic(String topicName, int partitions)
    {
        //int partitions = 8;
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient client = AdminClient.create(conf);

        short replicationFactor = 1;
        try {
            KafkaFuture<Void> future = client
                    .createTopics(Collections.singleton(new NewTopic(topicName, partitions,replicationFactor)),
                            new CreateTopicsOptions().timeoutMs(10000))
                    .all();
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        client.close();
    }

    public void deleteTopic() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient client = AdminClient.create(conf);
        KafkaFuture<Void> future = client.deleteTopics(Collections.singleton("tweet")).all();
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public void changeProperties() throws InterruptedException, ExecutionException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient client = AdminClient.create(conf);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "tweet");

        // get the current topic configuration
        DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, Config> config;
        config = describeConfigsResult.all().get();

        System.out.println(config);

        // create a new entry for updating the retention.ms value on the same topic
        ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "60000");
        Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
        updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

        AlterConfigsResult alterConfigsResult = client.alterConfigs(updateConfig);
        alterConfigsResult.all();

        describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

        config = describeConfigsResult.all().get();
    }
}
