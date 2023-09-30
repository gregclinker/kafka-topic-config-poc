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

@Setter
@Getter
@ToString
@NoArgsConstructor
@EqualsAndHashCode
public class EBTopicManagerConfig {

    private final static ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private String version;
    private String description;
    private List<EBTopicConfig> topicConfigs = new ArrayList<>();
    private Map<String, EBTopicConfig> topicConfigsMap = new HashMap<>();

    public EBTopicManagerConfig(InputStream inputStream) throws IOException {
        final EBTopicManagerConfig topicManagerConfig = objectMapper.readValue(inputStream, EBTopicManagerConfig.class);
        this.version = topicManagerConfig.version;
        this.description = topicManagerConfig.description;
        this.topicConfigs = topicManagerConfig.topicConfigs;
        topicConfigs.forEach(eTopicConfig -> {
            topicConfigsMap.put(eTopicConfig.getTopic(), eTopicConfig);
        });
    }

    public EBTopicManagerConfig getDelta(EBTopicManagerConfig newTopicManagerConfig) {
        EBTopicManagerConfig deltaTopicManagerConfig = new EBTopicManagerConfig();
        deltaTopicManagerConfig.setVersion(this.version);
        deltaTopicManagerConfig.setTopicConfigsMap(getDeltas(newTopicManagerConfig.topicConfigsMap));
        return deltaTopicManagerConfig;
    }

    public Map<String, EBTopicConfig> getDeltas(Map<String, EBTopicConfig> newTopicConfigsMap) {
        Map<String, EBTopicConfig> deltaETopicConfigsMap = new HashMap<>();
        newTopicConfigsMap.keySet().forEach(topic -> {
            if (topicConfigsMap.get(topic) == null) {
                deltaETopicConfigsMap.put(topic, newTopicConfigsMap.get(topic));
            } else if (!topicConfigsMap.get(topic).equals(newTopicConfigsMap.get(topic))) {
                deltaETopicConfigsMap.put(topic, new EBTopicConfig(topic));
                deltaETopicConfigsMap.get(topic).setConfigEntries(getDeltas(newTopicConfigsMap.get(topic)));

            }
        });
        return deltaETopicConfigsMap;
    }

    public List<EBTopicConfigEntry> getDeltas(EBTopicConfig newTopicConfig) {
        List<EBTopicConfigEntry> deltaETopicConfigEntries = new ArrayList<>();
        newTopicConfig.getConfigEntries().forEach(newETopicConfigEntry -> {
            if (topicConfigsMap.get(newETopicConfigEntry.getName()) == null || !topicConfigsMap.get(newETopicConfigEntry.getName()).equals(newETopicConfigEntry)) {
                deltaETopicConfigEntries.add(newETopicConfigEntry);
            }
        });
        return deltaETopicConfigEntries;
    }

    public void add(EBTopicConfig EBTopicConfig) {
        topicConfigs.add(EBTopicConfig);
        topicConfigsMap.put(EBTopicConfig.getTopic(), EBTopicConfig);
    }
}
