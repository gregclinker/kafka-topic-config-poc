package com.essexboy;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class ETopicManagerConfig {
    private String version;
    private List<ETopicConfig> topicConfigs=new ArrayList<>();
}
