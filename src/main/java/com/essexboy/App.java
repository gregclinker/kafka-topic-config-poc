package com.essexboy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    final static Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
        } catch (Exception e) {
            LOGGER.error("error", e);
            System.exit(-1);
        }
    }
}
