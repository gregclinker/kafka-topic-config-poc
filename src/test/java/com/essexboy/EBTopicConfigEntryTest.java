package com.essexboy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EBTopicConfigEntryTest {

    private EBTopicConfigEntry ebTopicConfigEntry = new EBTopicConfigEntry();

    @Test
    public void test1() {
        assertEquals("java.lang.String", ebTopicConfigEntry.parse("345s").getClass().getName());
        assertEquals("java.lang.Float", ebTopicConfigEntry.parse("345.2").getClass().getName());
        assertEquals("java.lang.Integer", ebTopicConfigEntry.parse("2147483647").getClass().getName());
        assertEquals("java.lang.Long", ebTopicConfigEntry.parse("2147483648").getClass().getName());
        assertEquals("java.lang.Boolean", ebTopicConfigEntry.parse("true").getClass().getName());
    }
}