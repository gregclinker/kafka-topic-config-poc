package com.essexboy;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AppTest {

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "172.18.0.3:29092")
    public void test1() throws Exception {
        AdminClient client = AdminClient.create(TopicManagerJobConfig.getConfig().getKafkaProperties());
        assertNotNull(client);

        final List<String> topics = client.listTopics().listings().get().stream().map(t -> t.name()).collect(Collectors.toList());
        System.out.println(topics);

        final List<ConfigResource> configResourceList = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList());
        topics.forEach(topic -> {
            try {
                final Map<ConfigResource, Config> configResourceConfigMap = client.describeConfigs(configResourceList).all().get();
                configResourceConfigMap.keySet().forEach(key -> {
                    final Config config = configResourceConfigMap.get(key);
                    config.entries().forEach(configEntry -> {
                        System.out.println("\"" + configEntry.name() + "\",\"" + configEntry.value() + "\"");
                    });
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }
}
