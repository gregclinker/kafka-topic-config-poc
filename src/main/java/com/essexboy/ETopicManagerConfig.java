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

@Getter
@Setter
@ToString
@NoArgsConstructor
@EqualsAndHashCode
public class ETopicManagerConfig {

    private final static ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private String version;
    private String description;
    private List<ETopicConfig> topicConfigs = new ArrayList<>();
    private Map<String, ETopicConfig> topicConfigsMap = new HashMap<>();

    public ETopicManagerConfig(InputStream inputStream) throws IOException {
        final ETopicManagerConfig topicManagerConfig = objectMapper.readValue(inputStream, ETopicManagerConfig.class);
        this.version = topicManagerConfig.version;
        this.description = topicManagerConfig.description;
        this.topicConfigs = topicManagerConfig.topicConfigs;
        topicConfigs.forEach(eTopicConfig -> {
            topicConfigsMap.put(eTopicConfig.getTopic(), eTopicConfig);
        });
    }

    public ETopicManagerConfig getDelta(ETopicManagerConfig newTopicManagerConfig) {
        ETopicManagerConfig deltaTopicManagerConfig = new ETopicManagerConfig();
        deltaTopicManagerConfig.setVersion(this.version);
        deltaTopicManagerConfig.setTopicConfigsMap(getDeltas(newTopicManagerConfig.topicConfigsMap)));
        return deltaTopicManagerConfig;
    }

    public Map<String, ETopicConfig> getDeltas(Map<String, ETopicConfig> newTopicConfigsMap) {
        Map<String, ETopicConfig> deltaETopicConfigsMap = new HashMap<>();
        newTopicConfigsMap.keySet().forEach(topic -> {
            if (topicConfigsMap.get(topic) == null) {
                deltaETopicConfigsMap.put(topic, newTopicConfigsMap.get(topic));
            } else if (!topicConfigsMap.get(topic).equals(newTopicConfigsMap.get(topic))) {
                deltaETopicConfigsMap.put(topic, new ETopicConfig());
                deltaETopicConfigsMap.get(topic).setConfigEntries(getDeltas(topicConfigsMap.get(topic), newTopicConfigsMap.get(topic)));

            }
        });
        return deltaETopicConfigsMap;
    }

    public List<ETopicConfigEntry> getDeltas(ETopicConfig existingTopicConfig, ETopicConfig newTopicConfig) {

        List<ETopicConfigEntry> deltaETopicConfigEntries = new ArrayList<>();

        newTopicConfig.getConfigEntries().forEach(eTopicConfigEntry -> {
            if (topicConfigsMap.get(eTopicConfigEntry.getName()) == null || !topicConfigsMap.get(eTopicConfigEntry.getName()).equals(eTopicConfigEntry)) {
                deltaETopicConfigEntries.add(eTopicConfigEntry);
            }
        });

        return deltaETopicConfigEntries;
    }

    public void add(ETopicConfig eTopicConfig) {
        topicConfigs.add(eTopicConfig);
        topicConfigsMap.put(eTopicConfig.getTopic(), eTopicConfig);
    }
}
