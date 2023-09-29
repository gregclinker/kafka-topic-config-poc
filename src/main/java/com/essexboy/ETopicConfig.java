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
public class ETopicConfig {
    private String topic;
    private int partitionCount;
    private int replicationFactor;
    private List<ETopicConfigEntry> configEntries = new ArrayList<>();

    public List<ETopicConfigEntry> getConfigEntries() {
        configEntries.sort(new Comparator<ETopicConfigEntry>() {
            @Override
            public int compare(ETopicConfigEntry o1, ETopicConfigEntry o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return configEntries;
    }

    public Map<String, ETopicConfigEntry> getConfigEntriesMap() {
        return getConfigEntries().stream().collect(Collectors.toMap(ETopicConfigEntry::getName, c -> c));
    }
}
