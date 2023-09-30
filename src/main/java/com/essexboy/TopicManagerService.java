package com.essexboy;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopicManagerService {

    final static Logger LOGGER = LoggerFactory.getLogger(TopicManagerService.class);

    public EBTopicManagerConfig get(AdminClient adminClient) throws InterruptedException, ExecutionException {
        EBTopicManagerConfig topicManagerConfig = new EBTopicManagerConfig();
        final List<String> topics = adminClient.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        final List<ConfigResource> configResourceList = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList());
        try {
            final Map<ConfigResource, Config> configResourceConfigMap = adminClient.describeConfigs(configResourceList).all().get();
            configResourceConfigMap.keySet().forEach(key -> {
                EBTopicConfig topicConfig = new EBTopicConfig(key.name());
                final Config config = configResourceConfigMap.get(key);
                config.entries().forEach(configEntry -> {
                    LOGGER.trace("topic={}, config={}, value={}", key.name(), configEntry.name(), configEntry.value());
                    topicConfig.getConfigEntries().add(new EBTopicConfigEntry(configEntry));
                });
                topicManagerConfig.add(topicConfig);
            });
            //find the partition count
            final Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(topics).allTopicNames().get();
            topicDescriptionMap.values().forEach(topicDescription -> {
                        topicManagerConfig.getTopicConfigsMap().get(topicDescription.name()).setPartitionCount(topicDescription.partitions().size());
                    }
            );
        } catch (Exception e) {
            LOGGER.error("error", e);
        }
        return topicManagerConfig;
    }

    public void alterTopicConfigs(EBTopicManagerConfig topicManagerConfig) throws Exception {
        try (AdminClient adminClient = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            final EBTopicManagerConfig existingTopicManagerConfig = get(adminClient);
            final EBTopicManagerConfig deltaTopicManagerConfig = existingTopicManagerConfig.getDelta(topicManagerConfig);
            final List<String> exisitingTopics = getAll();
            final Map<String, TopicDescription> existintTopicDescriptionMap = adminClient.describeTopics(exisitingTopics).allTopicNames().get();
            LOGGER.debug("delta config is {}", deltaTopicManagerConfig);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
            for (EBTopicConfig ebTopicConfig : deltaTopicManagerConfig.getTopicConfigs()) {
                final String topic = ebTopicConfig.getTopic();
                final int newPartitionCount = ebTopicConfig.getPartitionCount();
                if (!exisitingTopics.contains(topic)) {
                    NewTopic newTopic = new NewTopic(topic, newPartitionCount, (short) ebTopicConfig.getReplicationFactor());
                    adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                }
                if (existintTopicDescriptionMap.get(topic) != null) {
                    final int existingPartitionCount = existintTopicDescriptionMap.get(topic).partitions().size();
                    if (newPartitionCount > existingPartitionCount) {
                        Map<String, NewPartitions> newPartitionSet = new HashMap<>();
                        newPartitionSet.put(topic, NewPartitions.increaseTo(newPartitionCount));
                        adminClient.createPartitions(newPartitionSet).all().get();
                    } else if (newPartitionCount < existingPartitionCount) {
                        LOGGER.warn("can't decrease partition count from {} to {} fot topic {}", existingPartitionCount, newPartitionCount, topic);
                    }
                }
                final List<AlterConfigOp> alterConfigOps = new ArrayList<>();
                ebTopicConfig.getConfigEntries().forEach(eTopicConfigEntry -> {
                    alterConfigOps.add(new AlterConfigOp(new ConfigEntry(eTopicConfigEntry.getName(), eTopicConfigEntry.getValue().toString()), AlterConfigOp.OpType.SET));
                });
                configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), alterConfigOps);

            }
            adminClient.incrementalAlterConfigs(configs).all().get();
        }
    }

    public List<String> getAll() throws Exception {
        try (AdminClient client = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties())) {
            return client.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        }
    }
}
