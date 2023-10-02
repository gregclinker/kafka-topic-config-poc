package com.essexboy;

import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * POJO class to define the configuration of an individual topic.
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@EqualsAndHashCode
public class EBTopicConfig {
    private String topic;
    private int partitionCount;
    private int replicationFactor;
    private List<EBTopicConfigEntry> configEntries = new ArrayList<>();

    public EBTopicConfig(String topic) {
        this.topic = topic;
    }

    public Map<String, EBTopicConfigEntry> getConfigEntriesMap() {
        return getConfigEntries().stream().collect(Collectors.toMap(EBTopicConfigEntry::getName, c -> c));
    }
}
