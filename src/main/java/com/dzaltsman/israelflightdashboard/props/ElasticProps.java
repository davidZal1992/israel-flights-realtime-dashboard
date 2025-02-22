package com.dzaltsman.israelflightdashboard.props;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class ElasticProps {

    @Value("${elastic.index.name}")
    private String elasticIndex;
}