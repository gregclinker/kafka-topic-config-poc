package com.essexboy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EBTopicConfigEntryTest {

    private EBTopicConfigEntry EBTopicConfigEntry = new EBTopicConfigEntry();

    @Test
    public void test1() {
        assertEquals("java.lang.String", EBTopicConfigEntry.parse("345s").getClass().getName());
        assertEquals("java.lang.Float", EBTopicConfigEntry.parse("345.2").getClass().getName());
        assertEquals("java.lang.Integer", EBTopicConfigEntry.parse("2147483647").getClass().getName());
        assertEquals("java.lang.Long", EBTopicConfigEntry.parse("2147483648").getClass().getName());
        assertEquals("java.lang.Boolean", EBTopicConfigEntry.parse("true").getClass().getName());
    }
}