package com.essexboy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=STRING, documentation=null)
 * ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LIST, documentation=null)
 * ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null)
 * ConfigEntry(name=min.insync.replicas, value=2, source=DYNAMIC_TOPIC_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)
 * ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LIST, documentation=null)
 * ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LIST, documentation=null)
 * ConfigEntry(name=segment.bytes, value=1073741824, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)
 * ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=message.format.version, value=3.0-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=STRING, documentation=null)
 * ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)
 * ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=STRING, documentation=null)
 * ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null)
 * ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=DOUBLE, documentation=null)
 * ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)
 * ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=BOOLEAN, documentation=null)
 * ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=LONG, documentation=null)
 * ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[], type=INT, documentation=null)
 */
public class ETopicManagerConfigTest {

    @Test
    public void yaml() throws IOException {

        ETopicManagerConfig topicManagerConfig = new ETopicManagerConfig();
        topicManagerConfig.getTopicConfigs().add(getTopicConfig());

        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
        String yaml = objectMapper.writeValueAsString(topicManagerConfig);
        final ETopicManagerConfig topicManagerConfig1 = objectMapper.readValue(yaml, ETopicManagerConfig.class);
        assertTrue(topicManagerConfig.getTopicConfigs().equals(topicManagerConfig1.getTopicConfigs()));

        // check the delta between 2 & 3 is nothing
        final ETopicManagerConfig topicManagerConfig2 = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig2);
        final ETopicManagerConfig topicManagerConfig3 = new ETopicManagerConfig(getClass().getResourceAsStream("/test-topic-config1.yaml"));
        final ETopicManagerConfig topicManagerConfig3TopicManagerConfig2Delta = topicManagerConfig3.getDelta(topicManagerConfig2);
        topicManagerConfig3TopicManagerConfig2Delta.getTopicConfigsMap().values().forEach(eTopicConfig -> {
            assertEquals(0, eTopicConfig.getConfigEntries().size());
        });
    }

    @Test
    public void compare() throws IOException {
        ETopicConfig topicConfig1 = getTopicConfig();
        ETopicConfig topicConfig2 = getTopicConfig();
        assertTrue(topicConfig1.equals(topicConfig2));

        topicConfig2.getConfigEntries().set(3, new ETopicConfigEntry("min.insync.replicas", 3));
        assertFalse(topicConfig1.equals(topicConfig2));

        final ETopicManagerConfig topicManagerConfig1 = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig1);
        final ETopicManagerConfig topicManagerConfig2 = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig2);
        assertTrue(topicManagerConfig1.equals(topicManagerConfig2));
    }

    @Test
    public void deltas() throws IOException {
        final ETopicManagerConfig topicManagerConfig1 = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig1);

        final ETopicManagerConfig topicManagerConfig2 = new ETopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1-delta.yaml"));
        assertNotNull(topicManagerConfig2);

        final ETopicManagerConfig topicManagerConfig1TopicManagerConfig2Delta = topicManagerConfig1.getDelta(topicManagerConfig2);
        assertNotNull(topicManagerConfig1TopicManagerConfig2Delta);
        topicManagerConfig1TopicManagerConfig2Delta.getTopicConfigsMap().values().forEach(eTopicConfig -> {
            assertEquals(0, eTopicConfig.getConfigEntries().size());
        });
    }

    private ETopicConfig getTopicConfig() {
        ETopicConfig topicConfig = new ETopicConfig();
        topicConfig.setTopic("test-topic");
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("compression.type", "producer"));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("leader.replication.throttled.replicas", ""));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("message.downconversion.enable", true));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("min.insync.replicas", 2));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("segment.jitter.ms", 0));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("cleanup.policy", "delete"));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("flush.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("follower.replication.throttled.replicas", ""));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("segment.bytes", 1073741824));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("retention.ms", 604800000));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("flush.messages", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("message.format.version", "3.0-IV1"));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("max.compaction.lag.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("file.delete.delay.ms", 60000));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("max.message.bytes", 1048588));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("min.compaction.lag.ms", 0));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("message.timestamp.type", "CreateTime"));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("preallocate", false));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("min.cleanable.dirty.ratio", 0.5));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("index.interval.bytes", 4096));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("unclean.leader.election.enable", false));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("retention.bytes", -1));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("delete.retention.ms", 86400000));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("segment.ms", 604800000));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("message.timestamp.difference.max.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new ETopicConfigEntry("segment.index.bytes", 10485760));
        return topicConfig;
    }
}