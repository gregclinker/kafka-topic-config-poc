package com.essexboy;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Class to manage the configuration of the application.
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
public class TopicManagerJobConfig {

    final static Logger LOGGER = LoggerFactory.getLogger(TopicManagerJobConfig.class);

    private String version;
    private List<EBTopicConfig> topicConfigs;
    private Properties kafkaProperties;

    public static TopicManagerJobConfig getConfig() {
        final TopicManagerJobConfig topicManagerJobConfig = new TopicManagerJobConfig();
        Properties properties = new Properties();
        System.getenv().keySet().stream().filter(key -> key.startsWith("KAFKA_")).forEach(key -> {
            String kafkaProperty = key.replace("KAFKA_", "").replace("_", ".").toLowerCase();
            properties.put(kafkaProperty, System.getenv(key));
        });
        topicManagerJobConfig.setKafkaProperties(properties);
        LOGGER.debug("created config {}", topicManagerJobConfig);
        return topicManagerJobConfig;
    }
}

