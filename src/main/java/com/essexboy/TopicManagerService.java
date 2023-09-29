package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopicManagerService {

    final static Logger LOGGER = LoggerFactory.getLogger(TopicManagerService.class);

    public ETopicManagerConfig get(AdminClient adminClient) throws InterruptedException, ExecutionException, JsonProcessingException {
        ETopicManagerConfig topicManagerConfig = new ETopicManagerConfig();
        final List<String> topics = adminClient.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        final List<ConfigResource> configResourceList = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList());
        try {
            final Map<ConfigResource, Config> configResourceConfigMap = adminClient.describeConfigs(configResourceList).all().get();
            configResourceConfigMap.keySet().forEach(key -> {
                System.out.println(key.toString());
                ETopicConfig topicConfig = new ETopicConfig();
                topicConfig.setTopic(key.name());
                final Config config = configResourceConfigMap.get(key);
                config.entries().forEach(configEntry -> {
                    LOGGER.debug("topic={}, config={}, value={}", key.name(), configEntry.name(), configEntry.value());
                    topicConfig.getConfigEntries().add(new ETopicConfigEntry(configEntry));
                });
                topicManagerConfig.add(topicConfig);
            });
        } catch (Exception e) {
            LOGGER.error("error", e);
        }
        return topicManagerConfig;
    }

    public void alterTopicConfigs(ETopicManagerConfig topicManagerConfig) throws Exception {
        try (AdminClient adminClient = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            final ETopicManagerConfig existingTopicManagerConfig = get(adminClient);
            final ETopicManagerConfig deltaTopicManagerConfig = existingTopicManagerConfig.getDelta(topicManagerConfig);
            LOGGER.debug("delta config is {}", deltaTopicManagerConfig);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
            deltaTopicManagerConfig.getTopicConfigs().forEach(eTopicConfig -> {
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

    public List<String> getAll() throws Exception {
        try (AdminClient client = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            return client.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        }
    }
}
