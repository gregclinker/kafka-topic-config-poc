package com.essexboy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

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
public class EBTopicManagerConfigTest {

    @Test
    public void yaml() throws IOException {
        EBTopicManagerConfig topicManagerConfig = new EBTopicManagerConfig();
        topicManagerConfig.add(getTopicConfig());

        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
        String yaml = objectMapper.writeValueAsString(topicManagerConfig);
        final EBTopicManagerConfig topicManagerConfig1 = objectMapper.readValue(yaml, EBTopicManagerConfig.class);
        assertTrue(topicManagerConfig.getTopicConfigs().equals(topicManagerConfig1.getTopicConfigs()));

        // check the delta between 2 & 3 is nothing
        final EBTopicManagerConfig topicManagerConfig2 = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig2);
        final EBTopicManagerConfig topicManagerConfig3 = new EBTopicManagerConfig(getClass().getResourceAsStream("/test-topic-config1.yaml"));
        final EBTopicManagerConfig topicManagerConfig3TopicManagerConfig2Delta = topicManagerConfig3.getDelta(topicManagerConfig2);
        topicManagerConfig3TopicManagerConfig2Delta.getTopicConfigsMap().values().forEach(eTopicConfig -> {
            assertEquals(0, eTopicConfig.getConfigEntries().size());
        });
    }

    @Test
    public void compare() throws IOException {
        EBTopicConfig topicConfig1 = getTopicConfig();
        EBTopicConfig topicConfig2 = getTopicConfig();
        assertTrue(topicConfig1.equals(topicConfig2));

        topicConfig2.getConfigEntries().set(3, new EBTopicConfigEntry("min.insync.replicas", 3));
        assertFalse(topicConfig1.equals(topicConfig2));

        final EBTopicManagerConfig topicManagerConfig1 = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig1);
        final EBTopicManagerConfig topicManagerConfig2 = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig2);
        assertTrue(topicManagerConfig1.equals(topicManagerConfig2));
    }

    @Test
    public void deltas() throws IOException {
        final EBTopicManagerConfig topicManagerConfig1 = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1.yaml"));
        assertNotNull(topicManagerConfig1);
        assertEquals(1, topicManagerConfig1.getTopicConfigsMap().get("greg-test1").getPartitionCount());

        final EBTopicManagerConfig topicManagerConfig2 = new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1-delta.yaml"));
        assertNotNull(topicManagerConfig2);
        assertEquals(2, topicManagerConfig2.getTopicConfigsMap().get("greg-test2").getPartitionCount());

        final EBTopicManagerConfig topicManagerConfig1TopicManagerConfig2Delta = topicManagerConfig1.getDelta(topicManagerConfig2);
        assertNotNull(topicManagerConfig1TopicManagerConfig2Delta);
        final Map<String, EBTopicConfig> deltaTopicConfigsMap = topicManagerConfig1TopicManagerConfig2Delta.getTopicConfigsMap();
        EBTopicConfig topicConfig = deltaTopicConfigsMap.get("greg-test1");
        // no change to prtition
        assertEquals(0, topicConfig.getPartitionCount());
        // only change what's needed
        assertEquals(7, topicConfig.getConfigEntries().size());
        assertEquals(86400001, topicConfig.getConfigEntriesMap().get("delete.retention.ms").getValue());
        assertEquals(60001, topicConfig.getConfigEntriesMap().get("file.delete.delay.ms").getValue());
        assertEquals(604800001, topicConfig.getConfigEntriesMap().get("retention.ms").getValue());
        assertEquals(604800001, topicConfig.getConfigEntriesMap().get("segment.ms").getValue());
        assertEquals(4097, topicConfig.getConfigEntriesMap().get("index.interval.bytes").getValue());
        assertEquals(0.6, topicConfig.getConfigEntriesMap().get("min.cleanable.dirty.ratio").getValue());
        assertEquals(true, topicConfig.getConfigEntriesMap().get("unclean.leader.election.enable").getValue());

        // new topic
        topicConfig = deltaTopicConfigsMap.get("greg-test2");
        assertEquals(1, topicConfig.getConfigEntries().size());
        // set partitions (non zero)
        assertEquals(2, topicConfig.getPartitionCount());
        assertEquals(86400003, topicConfig.getConfigEntriesMap().get("delete.retention.ms").getValue());
    }

    @Test
    @Disabled
    public void badConfig() throws IOException {
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, () -> {
            new EBTopicManagerConfig(this.getClass().getResourceAsStream("/test-topic-config1-bad.yaml"));
        });
        Assertions.assertEquals("some message", thrown.getMessage());
    }

    private EBTopicConfig getTopicConfig() {
        EBTopicConfig topicConfig = new EBTopicConfig("test-topic");
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("compression.type", "producer"));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("leader.replication.throttled.replicas", ""));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("message.downconversion.enable", true));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("min.insync.replicas", 2));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("segment.jitter.ms", 0));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("cleanup.policy", "delete"));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("flush.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("follower.replication.throttled.replicas", ""));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("segment.bytes", 1073741824));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("retention.ms", 604800000));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("flush.messages", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("message.format.version", "3.0-IV1"));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("max.compaction.lag.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("file.delete.delay.ms", 60000));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("max.message.bytes", 1048588));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("min.compaction.lag.ms", 0));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("message.timestamp.type", "CreateTime"));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("preallocate", false));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("min.cleanable.dirty.ratio", 0.5));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("index.interval.bytes", 4096));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("unclean.leader.election.enable", false));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("retention.bytes", -1));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("delete.retention.ms", 86400000));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("segment.ms", 604800000));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("message.timestamp.difference.max.ms", 9223372036854775807L));
        topicConfig.getConfigEntries().add(new EBTopicConfigEntry("segment.index.bytes", 10485760));
        return topicConfig;
    }
}