package com.essexboy;

import lombok.*;
import org.apache.kafka.clients.admin.ConfigEntry;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class EBTopicConfigEntry {
    private String name;
    private Object value;

    public EBTopicConfigEntry(ConfigEntry configEntry) {
        this.name = configEntry.name();
        this.value = parse(configEntry.value());
    }

    public Object parse(String value) {
        if (value.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        } else if (value.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e1) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e2) {
                try {
                    return Float.parseFloat(value);
                } catch (NumberFormatException e3) {
                    try {
                        return Double.parseDouble(value);
                    } catch (NumberFormatException e4) {
                        return value;
                    }
                }
            }
        }
    }
}
