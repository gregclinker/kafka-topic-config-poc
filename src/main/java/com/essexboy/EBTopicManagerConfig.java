package com.essexboy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        return topicConfigsMap.values().stream().collect(Collectors.toList());
    }

    public void setTopicConfigs(List<EBTopicConfig> topicConfigs) {
        topicConfigsMap = new HashMap<>();
        topicConfigs.forEach(ebTopicConfig -> {
            topicConfigsMap.put(ebTopicConfig.getTopic(), ebTopicConfig);
        });
    }

    public EBTopicManagerConfig getDelta(EBTopicManagerConfig newTopicManagerConfig) {
        EBTopicManagerConfig deltaTopicManagerConfig = new EBTopicManagerConfig();
        deltaTopicManagerConfig.setVersion(this.version);
        deltaTopicManagerConfig.setTopicConfigsMap(getDeltas(newTopicManagerConfig.topicConfigsMap));
        return deltaTopicManagerConfig;
    }

    public Map<String, EBTopicConfig> getDeltas(Map<String, EBTopicConfig> newTopicConfigsMap) {
        Map<String, EBTopicConfig> deltaEbTopicConfigsMap = new HashMap<>();
        newTopicConfigsMap.keySet().forEach(topic -> {
            final EBTopicConfig existingEbTopicConfig = topicConfigsMap.get(topic);
            final EBTopicConfig newEbTopicConfig = newTopicConfigsMap.get(topic);
            if (existingEbTopicConfig == null) {
                deltaEbTopicConfigsMap.put(topic, newEbTopicConfig);
            } else if (!existingEbTopicConfig.equals(newEbTopicConfig)) {
                // if the partion count is to be change set it to non zero
                deltaEbTopicConfigsMap.put(topic, new EBTopicConfig(topic));
                if (existingEbTopicConfig.getPartitionCount() != newEbTopicConfig.getPartitionCount()) {
                    deltaEbTopicConfigsMap.get(topic).setPartitionCount(newEbTopicConfig.getPartitionCount());
                }
                deltaEbTopicConfigsMap.get(topic).setConfigEntries(getDeltas(newEbTopicConfig));

            }
        });
        return deltaEbTopicConfigsMap;
    }

    public List<EBTopicConfigEntry> getDeltas(EBTopicConfig newTopicConfig) {
        final String topic = newTopicConfig.getTopic();
        final Map<String, EBTopicConfigEntry> existingConfigEntriesMap = topicConfigsMap.get(topic).getConfigEntriesMap();
        final Map<String, EBTopicConfigEntry> newConfigEntriesMap = newTopicConfig.getConfigEntriesMap();
        final Map<String, EBTopicConfigEntry> deltaConfigEntriesMap = new HashMap<>();
        newConfigEntriesMap.keySet().forEach(key -> {
            final EBTopicConfigEntry exisitingEbTopicConfigEntry = existingConfigEntriesMap.get(key);
            final EBTopicConfigEntry newEbTopicConfigEntry = newConfigEntriesMap.get(key);
            if (exisitingEbTopicConfigEntry == null || !exisitingEbTopicConfigEntry.equals(newEbTopicConfigEntry)) {
                deltaConfigEntriesMap.put(key, newEbTopicConfigEntry);
            }

        });
        return deltaConfigEntriesMap.values().stream().collect(Collectors.toList());
    }

    public void add(EBTopicConfig EBTopicConfig) {
        topicConfigsMap.put(EBTopicConfig.getTopic(), EBTopicConfig);
    }
}
