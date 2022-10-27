package ru.mvz.elasticsearch.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.mvz.elasticsearch.util.DocumentBson;
import ru.mvz.elasticsearch.util.DocumentHelper;

import java.text.SimpleDateFormat;

@Configuration
@ConfigurationProperties(prefix = "app")
@Getter
@Setter
public class AppConfig {
    @Value("${:false}")
    private boolean hooksOnErrorDropped;
    @Value("${:20}")
    private int maxSizeBuffer;
    @Value("${:500}")
    private int maxDurationBuffer;
    @Value("${:2}")
    private int indexParallelism;
    @Value("${:100}")
    private int maxProcessingRequest;
    @Value("${:100}")
    private int sleepOverRequest;
    @Value("${:10}")
    private int sleepCycleCountMax;
    @Value("${:3}")
    private int webClientRetryMaxAttempts;
    @Value("${:2}")
    private int webClientRetryMinBackoff;
    @Value(":yyyy-MM-dd'T'HH:mm:ss.SSSXX")
    String dateFormat;

    @Bean
    DocumentHelper documentTree() {
        return new DocumentHelper(new DocumentBson(), objectMapper());
    }
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .setDateFormat(new SimpleDateFormat(dateFormat));
    }


}
