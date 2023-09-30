package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopicManagerServiceTest {

    private TopicManagerService topicManagerService = new TopicManagerService();

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:29092")
    public void alter1() throws Exception {
        setUp();
        EBTopicManagerConfig ebTopicManagerConfig = getETopicManagerConfig();
        assertNotNull(ebTopicManagerConfig);
        assertEquals(2, ebTopicManagerConfig.getTopicConfigsMap().values().size());
        assertEquals("java.lang.Integer", ebTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap().get("delete.retention.ms").getValue().getClass().getName());

        // check the alter config is correct
        ebTopicManagerConfig = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config-alter1.yaml"));
        assertEquals("java.lang.Integer", ebTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap().get("delete.retention.ms").getValue().getClass().getName());

        checkConfigAlter1(ebTopicManagerConfig);
        topicManagerService.alterTopicConfigs(ebTopicManagerConfig);
        // check the alter has been applied
        checkConfigAlter1(getETopicManagerConfig());
    }

    private void checkConfigAlter1(EBTopicManagerConfig ebTopicManagerConfig) {
        EBTopicConfig ebTopicConfig = ebTopicManagerConfig.getTopicConfigsMap().get("greg-test1");
        Map<String, EBTopicConfigEntry> ebTopicConfigConfigEntriesMap = ebTopicConfig.getConfigEntriesMap();
        assertEquals(86400000, ebTopicConfigConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60000, ebTopicConfigConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775807L, ebTopicConfigConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(false, ebTopicConfigConfigEntriesMap.get("message.downconversion.enable").getValue());

        ebTopicConfig = ebTopicManagerConfig.getTopicConfigsMap().get("greg-test2");
        ebTopicConfigConfigEntriesMap = ebTopicConfig.getConfigEntriesMap();
        assertEquals(86400001, ebTopicConfigConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60001, ebTopicConfigConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775806L, ebTopicConfigConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(true, ebTopicConfigConfigEntriesMap.get("message.downconversion.enable").getValue());
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:29092")
    public void alter2() throws Exception {
        setUp();
        // check the alter config is correct
        EBTopicManagerConfig ebTopicManagerConfig = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config-alter2.yaml"));
        assertEquals(3, ebTopicManagerConfig.getTopicConfigs().size());
        topicManagerService.alterTopicConfigs(ebTopicManagerConfig);
        // check the alter has been applied
        checkConfigAlter2(getETopicManagerConfig());
    }

    private void checkConfigAlter2(EBTopicManagerConfig ebTopicManagerConfig) {
        assertEquals(3, ebTopicManagerConfig.getTopicConfigs().size());

        EBTopicConfig ebTopicConfig = ebTopicManagerConfig.getTopicConfigsMap().get("greg-test1");
        Map<String, EBTopicConfigEntry> ebTopicConfigConfigEntriesMap = ebTopicConfig.getConfigEntriesMap();
        assertEquals(1, ebTopicConfig.getPartitionCount());
        assertEquals(86400000, ebTopicConfigConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60000, ebTopicConfigConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775807L, ebTopicConfigConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(false, ebTopicConfigConfigEntriesMap.get("message.downconversion.enable").getValue());

        ebTopicConfig = ebTopicManagerConfig.getTopicConfigsMap().get("greg-test2");
        ebTopicConfigConfigEntriesMap = ebTopicConfig.getConfigEntriesMap();
        assertEquals(2, ebTopicConfig.getPartitionCount());
        assertEquals(86400001, ebTopicConfigConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60001, ebTopicConfigConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775806L, ebTopicConfigConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(true, ebTopicConfigConfigEntriesMap.get("message.downconversion.enable").getValue());

        ebTopicConfig = ebTopicManagerConfig.getTopicConfigsMap().get("greg-test3");
        ebTopicConfigConfigEntriesMap = ebTopicConfig.getConfigEntriesMap();
        assertEquals(3, ebTopicConfig.getPartitionCount());
        assertEquals(86400002, ebTopicConfigConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60002, ebTopicConfigConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775806L, ebTopicConfigConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(false, ebTopicConfigConfigEntriesMap.get("message.downconversion.enable").getValue());
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:29092")
    public void badChange() throws Exception {
        setUp();
        ExecutionException thrown = Assertions.assertThrows(ExecutionException.class, () -> {
            EBTopicManagerConfig ebTopicManagerConfig = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1-bad.yaml"));
            topicManagerService.alterTopicConfigs(ebTopicManagerConfig);
        });
        assertEquals("org.apache.kafka.common.errors.InvalidConfigurationException: Unknown topic config name: delete.retention.ms.bad.config", thrown.getMessage());
    }

    private EBTopicManagerConfig getETopicManagerConfig() throws InterruptedException, ExecutionException, JsonProcessingException {
        try (AdminClient adminClient = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            return topicManagerService.get(adminClient);
        }
    }

    public void setUp() throws Exception {
        try (AdminClient client = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            final List<String> topics = client.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
            client.deleteTopics(topics).all().get();
            Thread.sleep(5000L);
            List<NewTopic> newTopics = new ArrayList<>();
            newTopics.add(new NewTopic("greg-test1", 1, (short) 1));
            newTopics.add(new NewTopic("greg-test2", 1, (short) 1));
            client.createTopics(newTopics).all().get();
        }
    }
}