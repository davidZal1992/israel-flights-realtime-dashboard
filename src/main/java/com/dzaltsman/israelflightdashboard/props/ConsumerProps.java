package com.dzaltsman.israelflightdashboard.props;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class ConsumerProps {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;


    @Value("${kafka.topic.name}")
    private String topicName;

}