package com.essexboy;

import lombok.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public List<EBTopicConfigEntry> getConfigEntries() {
        configEntries.sort(new Comparator<EBTopicConfigEntry>() {
            @Override
            public int compare(EBTopicConfigEntry o1, EBTopicConfigEntry o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return configEntries;
    }

    public Map<String, EBTopicConfigEntry> getConfigEntriesMap() {
        return getConfigEntries().stream().collect(Collectors.toMap(EBTopicConfigEntry::getName, c -> c));
    }
}
