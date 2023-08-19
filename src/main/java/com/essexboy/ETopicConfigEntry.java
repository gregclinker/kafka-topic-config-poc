package com.essexboy;

import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class ETopicConfigEntry {
    private String name;
    private Object value;
}
