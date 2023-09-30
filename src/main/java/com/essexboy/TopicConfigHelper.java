package com.essexboy;

import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.List;

public class TopicConfigHelper extends TopicConfig {

    private List<String> topicConfigs = new ArrayList<>();
    private EBTopicConfig ebTopicConfig;

    public TopicConfigHelper(EBTopicConfig ebTopicConfig) {
        super();
        this.ebTopicConfig = ebTopicConfig;
        topicConfigs.add(SEGMENT_BYTES_CONFIG);
        topicConfigs.add(SEGMENT_BYTES_DOC);
        topicConfigs.add(SEGMENT_MS_CONFIG);
        topicConfigs.add(SEGMENT_MS_DOC);
        topicConfigs.add(SEGMENT_JITTER_MS_CONFIG);
        topicConfigs.add(SEGMENT_JITTER_MS_DOC);
        topicConfigs.add(SEGMENT_INDEX_BYTES_CONFIG);
        topicConfigs.add(SEGMENT_INDEX_BYTES_DOC);
        topicConfigs.add(FLUSH_MESSAGES_INTERVAL_CONFIG);
        topicConfigs.add(FLUSH_MESSAGES_INTERVAL_DOC);
        topicConfigs.add(FLUSH_MS_CONFIG);
        topicConfigs.add(FLUSH_MS_DOC);
        topicConfigs.add(RETENTION_BYTES_CONFIG);
        topicConfigs.add(RETENTION_BYTES_DOC);
        topicConfigs.add(RETENTION_MS_CONFIG);
        topicConfigs.add(RETENTION_MS_DOC);
        topicConfigs.add(REMOTE_LOG_STORAGE_ENABLE_CONFIG);
        topicConfigs.add(REMOTE_LOG_STORAGE_ENABLE_DOC);
        topicConfigs.add(LOCAL_LOG_RETENTION_MS_CONFIG);
        topicConfigs.add(LOCAL_LOG_RETENTION_MS_DOC);
        topicConfigs.add(LOCAL_LOG_RETENTION_BYTES_CONFIG);
        topicConfigs.add(LOCAL_LOG_RETENTION_BYTES_DOC);
        topicConfigs.add(MAX_MESSAGE_BYTES_CONFIG);
        topicConfigs.add(MAX_MESSAGE_BYTES_DOC);
        topicConfigs.add(INDEX_INTERVAL_BYTES_CONFIG);
        topicConfigs.add(INDEX_INTERVAL_BYTES_DOCS);
        topicConfigs.add(FILE_DELETE_DELAY_MS_CONFIG);
        topicConfigs.add(FILE_DELETE_DELAY_MS_DOC);
        topicConfigs.add(DELETE_RETENTION_MS_CONFIG);
        topicConfigs.add(DELETE_RETENTION_MS_DOC);
        topicConfigs.add(MIN_COMPACTION_LAG_MS_CONFIG);
        topicConfigs.add(MIN_COMPACTION_LAG_MS_DOC);
        topicConfigs.add(MAX_COMPACTION_LAG_MS_CONFIG);
        topicConfigs.add(MAX_COMPACTION_LAG_MS_DOC);
        topicConfigs.add(MIN_CLEANABLE_DIRTY_RATIO_CONFIG);
        topicConfigs.add(MIN_CLEANABLE_DIRTY_RATIO_DOC);
        topicConfigs.add(CLEANUP_POLICY_CONFIG);
        topicConfigs.add(CLEANUP_POLICY_COMPACT);
        topicConfigs.add(CLEANUP_POLICY_DELETE);
        topicConfigs.add(CLEANUP_POLICY_DOC);
        topicConfigs.add(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
        topicConfigs.add(UNCLEAN_LEADER_ELECTION_ENABLE_DOC);
        topicConfigs.add(MIN_IN_SYNC_REPLICAS_CONFIG);
        topicConfigs.add(MIN_IN_SYNC_REPLICAS_DOC);
        topicConfigs.add(COMPRESSION_TYPE_CONFIG);
        topicConfigs.add(COMPRESSION_TYPE_DOC);
        topicConfigs.add(PREALLOCATE_CONFIG);
        topicConfigs.add(PREALLOCATE_DOC);
    }

    public void validate() {
        ebTopicConfig.getConfigEntries().forEach(ebTopicConfigEntry -> {
            if (!topicConfigs.contains(ebTopicConfigEntry.getName())) {
                throw new RuntimeException("ERROR " + ebTopicConfigEntry.getName() + " is invalid topc config");
            }
        });
    }
}
