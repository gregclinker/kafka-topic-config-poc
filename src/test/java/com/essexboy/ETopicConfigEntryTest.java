package com.essexboy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ETopicConfigEntryTest {

    private ETopicConfigEntry eTopicConfigEntry = new ETopicConfigEntry();

    @Test
    public void test1() {
        assertEquals("java.lang.String", eTopicConfigEntry.parse("345s").getClass().getName());
        assertEquals("java.lang.Float", eTopicConfigEntry.parse("345.2").getClass().getName());
        assertEquals("java.lang.Integer", eTopicConfigEntry.parse("2147483647").getClass().getName());
        assertEquals("java.lang.Long", eTopicConfigEntry.parse("2147483648").getClass().getName());
        assertEquals("java.lang.Boolean", eTopicConfigEntry.parse("true").getClass().getName());
    }
}