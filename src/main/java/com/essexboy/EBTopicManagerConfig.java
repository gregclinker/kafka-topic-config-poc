package com.essexboy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POJO class that defines the configuration of many topics.
 * <p>
 * Can be created from a yaml definition or a running Kafka cluster.
 * <p>
 * Provides utility methods to calculate the delta between 2 configurations.
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
@EqualsAndHashCode
public class EBTopicManagerConfig {

    private final static ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private String version;
    private String description;
    private Map<String, EBTopicConfig> topicConfigsMap = new HashMap<>();

    public EBTopicManagerConfig(InputStream inputStream) throws IOException {
        final EBTopicManagerConfig topicManagerConfig = objectMapper.readValue(inputStream, EBTopicManagerConfig.class);
        this.version = topicManagerConfig.version;
        this.description = topicManagerConfig.description;
        this.setTopicConfigs(topicManagerConfig.getTopicConfigs());
    }

    public List<EBTopicConfig> getTopicConfigs() {
        return new ArrayList<>(topicConfigsMap.values());
    }

    public void setTopicConfigs(List<EBTopicConfig> topicConfigs) {
        topicConfigsMap = new HashMap<>();
        topicConfigs.forEach(topicConfig -> topicConfigsMap.put(topicConfig.getTopic(), topicConfig));
    }

    public EBTopicManagerConfig getDelta(EBTopicManagerConfig newTopicManagerConfig) {
        EBTopicManagerConfig deltaTopicManagerConfig = new EBTopicManagerConfig();
        deltaTopicManagerConfig.setVersion(this.version);
        deltaTopicManagerConfig.setTopicConfigsMap(getDeltas(newTopicManagerConfig.topicConfigsMap));
        return deltaTopicManagerConfig;
    }

    private Map<String, EBTopicConfig> getDeltas(Map<String, EBTopicConfig> newTopicConfigsMap) {
        Map<String, EBTopicConfig> deltaTopicConfigsMap = new HashMap<>();
        newTopicConfigsMap.keySet().forEach(topic -> {
            final EBTopicConfig existingTopicConfig = topicConfigsMap.get(topic);
            final EBTopicConfig newTopicConfig = newTopicConfigsMap.get(topic);
            if (existingTopicConfig == null) {
                deltaTopicConfigsMap.put(topic, newTopicConfig);
            } else if (!existingTopicConfig.equals(newTopicConfig)) {
                // if the partion count is to be change set it to non zero
                deltaTopicConfigsMap.put(topic, new EBTopicConfig(topic));
                if (existingTopicConfig.getPartitionCount() != newTopicConfig.getPartitionCount()) {
                    deltaTopicConfigsMap.get(topic).setPartitionCount(newTopicConfig.getPartitionCount());
                }
                deltaTopicConfigsMap.get(topic).setConfigEntries(getDeltas(newTopicConfig));

            }
        });
        return deltaTopicConfigsMap;
    }

    private List<EBTopicConfigEntry> getDeltas(EBTopicConfig newTopicConfig) {
        final String topic = newTopicConfig.getTopic();
        final Map<String, EBTopicConfigEntry> existingConfigEntriesMap = topicConfigsMap.get(topic).getConfigEntriesMap();
        final Map<String, EBTopicConfigEntry> newConfigEntriesMap = newTopicConfig.getConfigEntriesMap();
        final Map<String, EBTopicConfigEntry> deltaConfigEntriesMap = new HashMap<>();
        newConfigEntriesMap.keySet().forEach(key -> {
            final EBTopicConfigEntry exisitingTopicConfigEntry = existingConfigEntriesMap.get(key);
            final EBTopicConfigEntry newTopicConfigEntry = newConfigEntriesMap.get(key);
            if (exisitingTopicConfigEntry == null || !exisitingTopicConfigEntry.equals(newTopicConfigEntry)) {
                deltaConfigEntriesMap.put(key, newTopicConfigEntry);
            }

        });
        return new ArrayList<>(deltaConfigEntriesMap.values());
    }

    public void add(EBTopicConfig EBTopicConfig) {
        topicConfigsMap.put(EBTopicConfig.getTopic(), EBTopicConfig);
    }
}
