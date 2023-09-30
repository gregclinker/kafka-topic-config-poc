package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AppTest {

    @Test
    @Disabled
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "172.18.0.3:29092")
    public void test1() throws Exception {
        AdminClient client = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties());
        final List<String> topics = client.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        client.deleteTopics(topics);

        List<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic("greg-test1", 3, (short) 1));
        newTopics.add(new NewTopic("greg-test2", 3, (short) 1));
        client.createTopics(newTopics);
    }

    private EBTopicManagerConfig showTopics() throws InterruptedException, ExecutionException, JsonProcessingException {
        EBTopicManagerConfig topicManagerConfig = new EBTopicManagerConfig();
        try (AdminClient adminClient = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            final List<String> topics = adminClient.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
            System.out.println(topics);
            final List<ConfigResource> configResourceList = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList());
            topics.forEach(topic -> {
                EBTopicConfig topicConfig = new EBTopicConfig(topic);
                try {
                    final Map<ConfigResource, Config> configResourceConfigMap = adminClient.describeConfigs(configResourceList).all().get();
                    configResourceConfigMap.keySet().forEach(key -> {
                        final Config config = configResourceConfigMap.get(key);
                        config.entries().forEach(configEntry -> {
                            System.out.println("topic=" + topic + ", config=" + configEntry.name() + ", value=" + configEntry.value());
                            topicConfig.getConfigEntries().add(new EBTopicConfigEntry(configEntry));
                        });
                    });
                    topicManagerConfig.add(topicConfig);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
        return topicManagerConfig;
    }

    @Test
    @Disabled
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "172.18.0.3:29092")
    public void test2() throws Exception {
        EBTopicManagerConfig topicManagerConfig = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig);
        alterTopicConfigs(topicManagerConfig);
        showTopics();

        topicManagerConfig = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config2.yaml"));
        assertNotNull(topicManagerConfig);
        alterTopicConfigs(topicManagerConfig);
        showTopics();
    }

    private void alterTopicConfigs(EBTopicManagerConfig topicManagerConfig) throws IOException, InterruptedException, ExecutionException {
        try (AdminClient adminClient = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
            topicManagerConfig.getTopicConfigs().forEach(eTopicConfig -> {
                final List<AlterConfigOp> alterConfigOps = new ArrayList<>();
                eTopicConfig.getConfigEntries().forEach(eTopicConfigEntry -> {
                    alterConfigOps.add(new AlterConfigOp(new ConfigEntry(eTopicConfigEntry.getName(), eTopicConfigEntry.getValue().toString()), AlterConfigOp.OpType.SET));
                });
                configs.put(new ConfigResource(ConfigResource.Type.TOPIC, eTopicConfig.getTopic()), alterConfigOps);
            });
            final KafkaFuture<Void> all = adminClient.incrementalAlterConfigs(configs).all();
            while (!all.isDone()) {
                Thread.sleep(100);
            }
            all.get();
        }
    }
}
