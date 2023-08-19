package com.essexboy;

import lombok.Getter;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;

import java.util.List;
import java.util.stream.Collectors;

@Getter
public class TopicInfo {
    private String name;
    private Config config;
    private List<PartitionInfo> partitions;

    public TopicInfo(TopicDescription topicDescription, Config config) {
        this.name = topicDescription.name();
        this.config = config;
        this.partitions = topicDescription.partitions().stream().map(PartitionInfo::new).collect(Collectors.toList());
    }

    public TopicInfo(TopicDescription topicDescription) {
        this.name = topicDescription.name();
        this.partitions = topicDescription.partitions().stream().map(PartitionInfo::new).collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public int getPartitionMinIsr() {
        return partitions.stream().map(p -> p.getIsrs().size()).min(Integer::compareTo).get();
    }

    public int getMinReplicas() {
        return partitions.stream().map(p -> p.getReplicas().size()).min(Integer::compareTo).get();
    }

    public List<PartitionInfo> getPartitionsLessThanReplicationFactor(int rf) {
        return partitions.stream().filter(p -> p.getReplicas().size() < rf).collect(Collectors.toList());
    }

    public List<PartitionInfo> getPartitionsLessThanIsr(int isr) {
        return partitions.stream().filter(p -> p.getIsrs().size() < isr).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TopicInfo{");
        sb.append("name='").append(name).append('\'');
        sb.append(", partitions=").append(partitions);
        sb.append('}');
        return sb.toString();
    }
}
