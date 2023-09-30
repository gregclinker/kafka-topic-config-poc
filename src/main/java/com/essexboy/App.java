package com.essexboy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class App {

    final static Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.out.println("");
                System.out.println("Usage topic <yaml config file>");
                System.out.println("");
                System.exit(0);
            } else {
                final TopicManagerService topicManagerService = new TopicManagerService();
                System.out.println(args[0]);
                final EBTopicManagerConfig ebTopicManagerConfig = new EBTopicManagerConfig(new FileInputStream(args[0]));
                topicManagerService.alterTopicConfigs(ebTopicManagerConfig);
                System.exit(0);
            }
        } catch (Exception e) {
            LOGGER.error("error", e);
            System.exit(-1);
        }
    }
}
