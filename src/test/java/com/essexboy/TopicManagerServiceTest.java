package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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
    public void test() throws Exception {
        setUp();

        ETopicManagerConfig eTopicManagerConfig = getETopicManagerConfig();
        assertNotNull(eTopicManagerConfig);
        assertEquals(2, eTopicManagerConfig.getTopicConfigsMap().values().size());
        assertEquals("java.lang.Integer", eTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap().get("delete.retention.ms").getValue().getClass().getName());

        // check the alter config is correct
        eTopicManagerConfig = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config-alter1.yaml"));
        assertEquals("java.lang.Integer", eTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap().get("delete.retention.ms").getValue().getClass().getName());

        Map<String, ETopicConfigEntry> topic1ConfigEntriesMap = eTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap();
        assertEquals(86400000, topic1ConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60000, topic1ConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775807L, topic1ConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(false, topic1ConfigEntriesMap.get("message.downconversion.enable").getValue());

        Map<String, ETopicConfigEntry> topic2ConfigEntriesMap = eTopicManagerConfig.getTopicConfigsMap().get("greg-test2").getConfigEntriesMap();
        assertEquals(86400001, topic2ConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60001, topic2ConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775806L, topic2ConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(true, topic2ConfigEntriesMap.get("message.downconversion.enable").getValue());
        topicManagerService.alterTopicConfigs(eTopicManagerConfig);

        // check the alter has been applied
        Thread.sleep(5000L);
        eTopicManagerConfig = getETopicManagerConfig();
        System.out.println(eTopicManagerConfig);

        topic1ConfigEntriesMap = eTopicManagerConfig.getTopicConfigsMap().get("greg-test1").getConfigEntriesMap();
        assertEquals("java.lang.Integer", topic1ConfigEntriesMap.get("delete.retention.ms").getValue().getClass().getName());
        assertEquals(86400000, topic1ConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60000, topic1ConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775807L, topic1ConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(false, topic1ConfigEntriesMap.get("message.downconversion.enable").getValue());

        topic2ConfigEntriesMap = eTopicManagerConfig.getTopicConfigsMap().get("greg-test2").getConfigEntriesMap();
        assertEquals(86400001, topic2ConfigEntriesMap.get("delete.retention.ms").getValue());
        assertEquals(60001, topic2ConfigEntriesMap.get("file.delete.delay.ms").getValue());
        assertEquals(9223372036854775806L, topic2ConfigEntriesMap.get("flush.messages").getValue());
        assertEquals(true, topic2ConfigEntriesMap.get("message.downconversion.enable").getValue());
    }

    private ETopicManagerConfig getETopicManagerConfig() throws InterruptedException, ExecutionException, JsonProcessingException {
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
            newTopics.add(new NewTopic("greg-test1", 3, (short) 1));
            newTopics.add(new NewTopic("greg-test2", 3, (short) 1));
            client.createTopics(newTopics).all().get();
        }
    }
}